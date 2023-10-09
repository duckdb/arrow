#include "arrow_scan_ipc.hpp"

namespace duckdb {

TableFunction ArrowIPCTableFunction::GetFunction() {
    child_list_t <LogicalType> make_buffer_struct_children{{"ptr",  LogicalType::UBIGINT},
                                                           {"size", LogicalType::UBIGINT}};

    TableFunction scan_arrow_ipc_func(
            "scan_arrow_ipc", {LogicalType::LIST(LogicalType::STRUCT(make_buffer_struct_children))},
        ArrowIPCTableFunction::ArrowScanFunction, ArrowIPCTableFunction::ArrowScanBind,
        ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);

    scan_arrow_ipc_func.cardinality = ArrowTableFunction::ArrowScanCardinality;
    scan_arrow_ipc_func.get_batch_index = nullptr; // TODO implement
    scan_arrow_ipc_func.projection_pushdown = true;
    scan_arrow_ipc_func.filter_pushdown = false;

    return scan_arrow_ipc_func;
}

unique_ptr <FunctionData> ArrowIPCTableFunction::ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector <LogicalType> &return_types, vector <string> &names) {
    auto stream_decoder = make_uniq<BufferingArrowIPCStreamDecoder>();

    // Decode buffer ptr list
    auto buffer_ptr_list = ListValue::GetChildren(input.inputs[0]);
    for (auto &buffer_ptr_struct: buffer_ptr_list) {
        auto unpacked = StructValue::GetChildren(buffer_ptr_struct);
        uint64_t ptr = unpacked[0].GetValue<uint64_t>();
        uint64_t size = unpacked[1].GetValue<uint64_t>();

        // Feed stream into decoder
        auto res = stream_decoder->Consume((const uint8_t *) ptr, size);

        if (!res.ok()) {
            throw IOException("Invalid IPC stream");
        }
    }

    if (!stream_decoder->buffer()->is_eos()) {
        throw IOException("IPC buffers passed to arrow scan should contain entire stream");
    }

    // These are the params I need to produce from the ipc buffers using the WebDB.cc code
    auto stream_factory_ptr = (uintptr_t) & stream_decoder->buffer();
    auto stream_factory_produce = (stream_factory_produce_t) & ArrowIPCStreamBufferReader::CreateStream;
    auto stream_factory_get_schema = (stream_factory_get_schema_t) & ArrowIPCStreamBufferReader::GetSchema;
    auto res = make_uniq<ArrowIPCScanFunctionData>(stream_factory_produce, stream_factory_ptr);

    // Store decoder
    res->stream_decoder = std::move(stream_decoder);

    // TODO Everything below this is identical to the bind in duckdb/src/function/table/arrow.cpp
    auto &data = *res;
    stream_factory_get_schema(stream_factory_ptr, data.schema_root);
    for (idx_t col_idx = 0; col_idx < (idx_t) data.schema_root.arrow_schema.n_children; col_idx++) {
        auto &schema = *data.schema_root.arrow_schema.children[col_idx];
        if (!schema.release) {
            throw InvalidInputException("arrow_scan: released schema passed");
        }
        auto arrow_type = GetArrowLogicalType(schema);
        if (schema.dictionary) {
            auto dictionary_type = GetArrowLogicalType(*schema.dictionary);
            return_types.emplace_back(dictionary_type->GetDuckType());
            arrow_type->SetDictionary(std::move(dictionary_type));
        } else {
            return_types.emplace_back(arrow_type->GetDuckType());
        }
        res->arrow_table.AddColumn(col_idx, std::move(arrow_type));
        auto format = string(schema.format);
        auto name = string(schema.name);
        if (name.empty()) {
            name = string("v") + to_string(col_idx);
        }
        names.push_back(name);
    }
    ArrowTableFunction::RenameArrowColumns(names);
    return std::move(res);
}

// Same as regular arrow scan, except ArrowToDuckDB call TODO: refactor to allow nicely overriding this
void ArrowIPCTableFunction::ArrowScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    if (!data_p.local_state) {
        return;
    }
    auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
    auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

    //! Out of tuples in this chunk
    if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
        if (!ArrowScanParallelStateNext(context, data_p.bind_data.get(), state, global_state)) {
            return;
        }
    }
    int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
    data.lines_read += output_size;
    if (global_state.CanRemoveFilterColumns()) {
        state.all_columns.Reset();
        state.all_columns.SetCardinality(output_size);
        ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns, data.lines_read - output_size, false);
        output.ReferenceColumns(state.all_columns, global_state.projection_ids);
    } else {
        output.SetCardinality(output_size);
        ArrowToDuckDB(state, data.arrow_table.GetColumns(), output, data.lines_read - output_size, false);
    }

    output.Verify();
    state.chunk_offset += output.size();
}

} // namespace duckdb