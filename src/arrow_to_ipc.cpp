#include "arrow_to_ipc.hpp"

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

#include <memory>

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_properties.hpp"
#endif

namespace duckdb {

struct ToArrowIpcFunctionData : public TableFunctionData {
  ToArrowIpcFunctionData() {}
  std::shared_ptr<arrow::Schema> schema;
  idx_t chunk_size;
};

struct ToArrowIpcGlobalState : public GlobalTableFunctionState {
  ToArrowIpcGlobalState() : sent_schema(false) {}
  atomic<bool> sent_schema;
  mutex lock;
};

struct ToArrowIpcLocalState : public LocalTableFunctionState {
  unique_ptr<ArrowAppender> appender;
  idx_t current_count = 0;
  bool checked_schema = false;
};

unique_ptr<LocalTableFunctionState>
ToArrowIPCFunction::InitLocal(ExecutionContext &context,
                              TableFunctionInitInput &input,
                              GlobalTableFunctionState *global_state) {
  return make_uniq<ToArrowIpcLocalState>();
}

unique_ptr<GlobalTableFunctionState>
ToArrowIPCFunction::InitGlobal(ClientContext &context,
                               TableFunctionInitInput &input) {
  return make_uniq<ToArrowIpcGlobalState>();
}

unique_ptr<FunctionData>
ToArrowIPCFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                         vector<LogicalType> &return_types,
                         vector<string> &names) {
  auto result = make_uniq<ToArrowIpcFunctionData>();

  result->chunk_size = DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;

  // Set return schema
  return_types.emplace_back(LogicalType::BLOB);
  names.emplace_back("ipc");
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("header");

  // Create the Arrow schema
  ArrowSchema schema;
  ArrowConverter::ToArrowSchema(&schema, input.input_table_types,
                                input.input_table_names,
                                context.GetClientProperties());
  result->schema = arrow::ImportSchema(&schema).ValueOrDie();

  return std::move(result);
}

OperatorResultType ToArrowIPCFunction::Function(ExecutionContext &context,
                                                TableFunctionInput &data_p,
                                                DataChunk &input,
                                                DataChunk &output) {
  std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;
  auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
  auto &local_state = (ToArrowIpcLocalState &)*data_p.local_state;
  auto &global_state = (ToArrowIpcGlobalState &)*data_p.global_state;

  bool sending_schema = false;

  bool caching_disabled = !PhysicalOperator::OperatorCachingAllowed(context);

  if (!local_state.checked_schema) {
    if (!global_state.sent_schema) {
      lock_guard<mutex> init_lock(global_state.lock);
      if (!global_state.sent_schema) {
        // This run will send the schema, other threads can just send the
        // buffers
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
      local_state.appender =
          make_uniq<ArrowAppender>(input.GetTypes(), data.chunk_size,
                                   context.client.GetClientProperties());
    }

    // Append input chunk
    local_state.appender->Append(input, 0, input.size(), input.size());
    local_state.current_count += input.size();

    // If chunk size is reached, we can flush to IPC blob
    if (caching_disabled || local_state.current_count >= data.chunk_size) {
      // Construct record batch from DataChunk
      ArrowArray arr = local_state.appender->Finalize();
      auto record_batch =
          arrow::ImportRecordBatch(&arr, data.schema).ValueOrDie();

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
  auto wrapped_buffer =
      make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);
  auto &vector = output.data[0];
  StringVector::AddBuffer(vector, wrapped_buffer);
  auto data_ptr = (string_t *)vector.GetData();
  *data_ptr = string_t((const char *)arrow_serialized_ipc_buffer->data(),
                       arrow_serialized_ipc_buffer->size());
  output.SetCardinality(1);

  if (sending_schema) {
    return OperatorResultType::HAVE_MORE_OUTPUT;
  } else {
    return OperatorResultType::NEED_MORE_INPUT;
  }
}

OperatorFinalizeResultType ToArrowIPCFunction::FunctionFinal(
    ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
  auto &local_state = (ToArrowIpcLocalState &)*data_p.local_state;
  std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;

  // TODO clean up
  if (local_state.appender) {
    ArrowArray arr = local_state.appender->Finalize();
    auto record_batch =
        arrow::ImportRecordBatch(&arr, data.schema).ValueOrDie();

    // Serialize recordbatch
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    auto result = arrow::ipc::SerializeRecordBatch(*record_batch, options);
    arrow_serialized_ipc_buffer = result.ValueOrDie();

    auto wrapped_buffer =
        make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);
    auto &vector = output.data[0];
    StringVector::AddBuffer(vector, wrapped_buffer);
    auto data_ptr = (string_t *)vector.GetData();
    *data_ptr = string_t((const char *)arrow_serialized_ipc_buffer->data(),
                         arrow_serialized_ipc_buffer->size());
    output.SetCardinality(1);
    local_state.appender.reset();
    output.data[1].SetValue(0, Value::BOOLEAN(0));
  }

  return OperatorFinalizeResultType::FINISHED;
}

TableFunction ToArrowIPCFunction::GetFunction() {
  TableFunction fun("to_arrow_ipc", {LogicalType::TABLE}, nullptr,
                    ToArrowIPCFunction::Bind, ToArrowIPCFunction::InitGlobal,
                    ToArrowIPCFunction::InitLocal);
  fun.in_out_function = ToArrowIPCFunction::Function;
  fun.in_out_function_final = ToArrowIPCFunction::FunctionFinal;

  return fun;
}

} // namespace duckdb