#define DUCKDB_EXTENSION_MAIN

#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/type_fwd.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/c/bridge.h"
#include "arrow_extension.hpp"
#include "arrow_stream_buffer.hpp"
#include "arrow_scan_ipc.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table/arrow.hpp"
#endif

namespace duckdb {

class ArrowStringVectorBuffer : public VectorBuffer {
public:
	explicit ArrowStringVectorBuffer(std::shared_ptr<arrow::Buffer> buffer_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(move(buffer_p)) {
	}

private:
	std::shared_ptr<arrow::Buffer> buffer;
};

//! note: this is the number of vectors per chunk
static constexpr idx_t DEFAULT_CHUNK_SIZE = 120;

struct ToArrowIpcFunctionData : public TableFunctionData {
	ToArrowIpcFunctionData() {
	}
	shared_ptr<arrow::Schema> schema;
	idx_t chunk_size;
};

struct ToArrowIpcGlobalState : public GlobalTableFunctionState {
	ToArrowIpcGlobalState() : sent_schema(false) {
	}
	atomic<bool> sent_schema;
	mutex lock;
};

struct ToArrowIpcLocalState : public LocalTableFunctionState {
	unique_ptr<ArrowAppender> appender;
	idx_t current_count = 0;
	bool checked_schema = false;
};

static unique_ptr<LocalTableFunctionState> ToArrowIpcInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_unique<ToArrowIpcLocalState>();
}

static unique_ptr<GlobalTableFunctionState> ToArrowIpcInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_unique<ToArrowIpcGlobalState>();
}

static unique_ptr<FunctionData> ToArrowIpcBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<ToArrowIpcFunctionData>();

	result->chunk_size = DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;

	// Set return schema
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("ipc");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("header");

	// Create the Arrow schema
	auto tz = context.GetClientProperties().timezone;
	ArrowSchema schema;
	ArrowConverter::ToArrowSchema(&schema, input.input_table_types, input.input_table_names, tz);
	result->schema = arrow::ImportSchema(&schema).ValueOrDie();

	return move(result);
}

static OperatorResultType ToArrowIpcFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                             DataChunk &output) {
	std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;
	auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
	auto &local_state = (ToArrowIpcLocalState &)*data_p.local_state;
	auto &global_state = (ToArrowIpcGlobalState &)*data_p.global_state;

	bool sending_schema = false;

	if (!local_state.checked_schema) {
		if (!global_state.sent_schema) {
			lock_guard<mutex> init_lock(global_state.lock);
			if (!global_state.sent_schema) {
				// This run will send the schema, other threads can just send the buffers
				global_state.sent_schema = true;
				sending_schema = true;
			}
		}
		local_state.checked_schema = true;
	}

	if (sending_schema) {
		auto result = arrow::ipc::SerializeSchema(*data.schema);
		arrow_serialized_ipc_buffer = result.ValueOrDie();
		output.data[1].SetValue(0, Value::BOOLEAN(1));
	} else {
		if (!local_state.appender) {
			local_state.appender = make_unique<ArrowAppender>(input.GetTypes(), data.chunk_size);
		}

		// Append input chunk
		local_state.appender->Append(input);
		local_state.current_count += input.size();

		// If chunk size is reached, we can flush to IPC blob
		if (local_state.current_count >= data.chunk_size) {
			// Construct record batch from DataChunk
			ArrowArray arr = local_state.appender->Finalize();
			auto record_batch = arrow::ImportRecordBatch(&arr, data.schema).ValueOrDie();

			// Serialize recordbatch
			auto options = arrow::ipc::IpcWriteOptions::Defaults();
			auto result = arrow::ipc::SerializeRecordBatch(*record_batch, options);
			arrow_serialized_ipc_buffer = result.ValueOrDie();

			// Reset appender
			local_state.appender.reset();
			local_state.current_count = 0;

			output.data[1].SetValue(0, Value::BOOLEAN(0));
		} else {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}

	// TODO clean up
	auto wrapped_buffer = make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);
	auto &vector = output.data[0];
	StringVector::AddBuffer(vector, wrapped_buffer);
	auto data_ptr = (string_t *)vector.GetData();
	*data_ptr = string_t((const char *)arrow_serialized_ipc_buffer->data(), arrow_serialized_ipc_buffer->size());
	output.SetCardinality(1);

	if (sending_schema) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

static OperatorFinalizeResultType ToArrowIpcFunctionFinal(ExecutionContext &context, TableFunctionInput &data_p,
                                                          DataChunk &output) {
	auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
	auto &local_state = (ToArrowIpcLocalState &)*data_p.local_state;
	std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;

	// TODO clean up
	if (local_state.appender) {
		ArrowArray arr = local_state.appender->Finalize();
		auto record_batch = arrow::ImportRecordBatch(&arr, data.schema).ValueOrDie();

		// Serialize recordbatch
		auto options = arrow::ipc::IpcWriteOptions::Defaults();
		auto result = arrow::ipc::SerializeRecordBatch(*record_batch, options);
		arrow_serialized_ipc_buffer = result.ValueOrDie();

		auto wrapped_buffer = make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);
		auto &vector = output.data[0];
		StringVector::AddBuffer(vector, wrapped_buffer);
		auto data_ptr = (string_t *)vector.GetData();
		*data_ptr = string_t((const char *)arrow_serialized_ipc_buffer->data(), arrow_serialized_ipc_buffer->size());
		output.SetCardinality(1);
		local_state.appender.reset();
		output.data[1].SetValue(0, Value::BOOLEAN(0));
	}

	return OperatorFinalizeResultType::FINISHED;
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

    // Register to_arrow_ipc
	TableFunction to_arrow_ipc_func("to_arrow_ipc", {LogicalType::TABLE}, nullptr, ToArrowIpcBind,
	                                 ToArrowIpcInitGlobal, ToArrowIpcInitLocal);
	to_arrow_ipc_func.in_out_function = ToArrowIpcFunction;
	to_arrow_ipc_func.in_out_function_final = ToArrowIpcFunctionFinal;
	CreateTableFunctionInfo to_arrow_ipc_info(to_arrow_ipc_func);
	catalog.CreateTableFunction(*con.context, &to_arrow_ipc_info);

    // Register scan_arrow_ipc
	CreateTableFunctionInfo scan_arrow_ipc_info(ArrowIPCTableFunction::GetFunction());
	catalog.CreateTableFunction(*con.context, &scan_arrow_ipc_info);

	con.Commit();
}

void ArrowExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ArrowExtension::Name() {
	return "arrow";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void arrow_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *arrow_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
