#include "arrow_scan_ipc.hpp"

namespace duckdb {

  arrow::Status
  ConsumeArrowStream( std::shared_ptr<ArrowIPCStreamBuffer> ipc_buffer
                     ,std::vector<duckdb::Value> input_buffers) {
    // Decoder consumes each IPC buffer and the `ipc_buffer` listens for its events
    auto stream_decoder = make_uniq<ArrowIPCDec>(std::move(ipc_buffer));
    for (auto& input_buffer: input_buffers) {
      // Each "buffer" is a pointer to data and a buffer size
      auto     unpacked    = StructValue::GetChildren(input_buffer);
      uint64_t buffer_ptr  = unpacked[0].GetValue<uint64_t>();
      uint64_t buffer_size = unpacked[1].GetValue<uint64_t>();

      auto decode_status = stream_decoder->Consume(
        (const uint8_t*) buffer_ptr, buffer_size
      );

      if (!decode_status.ok()) { return decode_status; }
    }

    return arrow::Status::OK();
  }

  unique_ptr<FunctionData>
  ArrowIPCTableFunction::ArrowScanBind( ClientContext&          context
                                       ,TableFunctionBindInput& input
                                       ,vector <LogicalType>&   return_types
                                       ,vector <string>&        names) {
    // Extract function arguments
    auto input_buffers = ListValue::GetChildren(input.inputs[0]);

    // TODO: why do all of this at binding time?
    // Start by reading all data from input IPC buffers
    auto ipc_buffer    = std::make_shared<ArrowIPCStreamBuffer>();
    auto decode_status = ConsumeArrowStream(ipc_buffer, input_buffers);

    // Errors if status is not OK or if the decoder never saw an end-of-stream marker
    if (!decode_status.ok())   { throw IOException("Invalid IPC stream"); }
    if (!ipc_buffer->is_eos()) { throw IOException("arrow scan expects entire IPC stream"); }

    // Maintain the Arrow data (IPC buffer) and a function to create a RecordBatchReader
    auto reader_factory = (stream_factory_produce_t) &CStreamForIPCBuffer;
    auto fn_data        = make_uniq<ArrowIPCScanFunctionData>(reader_factory, ipc_buffer);

    // First, decode the schema from the IPC stream into `fn_data`
    // Then, bind the logical schema types (`names` and `return_types`)
    SchemaFromIPCBuffer(ipc_buffer, fn_data->schema_root);
    PopulateArrowTableType(fn_data->arrow_table, fn_data->schema_root, names, return_types);

    QueryResult::DeduplicateColumns(names);
    return std::move(fn_data);
  }

  // Same as regular arrow scan, except ArrowToDuckDB call
  // TODO: refactor to allow nicely overriding this
  void ArrowIPCTableFunction::ArrowScanFunction( ClientContext&      context
                                                ,TableFunctionInput& data_p
                                                ,DataChunk&          output) {
    // Check for early exits (empty local state or no more tuples)
    if (!data_p.local_state) { return; }

    auto &data         = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
    auto &state        = data_p.local_state->Cast<ArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

    if (state.chunk_offset >= (idx_t) state.chunk->arrow_array.length) {
      if (!ArrowScanParallelStateNext(context, data_p.bind_data.get(), state, global_state)) {
        return;
      }
    }

    // Move (zero-copy) Arrow data to DuckDB chunk
    int64_t output_size = MinValue<int64_t>(
       STANDARD_VECTOR_SIZE
      ,state.chunk->arrow_array.length - state.chunk_offset
    );
    data.lines_read += output_size;

    // For now, maintain that we did not pushdown projection into Arrow.
    // I think this means that it was not pushed down into the scan? Here, since we're
    // only getting IPC buffers, then there was no scan for us to push down into.
    constexpr bool was_proj_pusheddown { false };

    if (global_state.CanRemoveFilterColumns()) {
        state.all_columns.Reset();
        state.all_columns.SetCardinality(output_size);

        ArrowToDuckDB( state
                      ,data.arrow_table.GetColumns()
                      ,state.all_columns
                      ,data.lines_read - output_size
                      ,was_proj_pusheddown);

        output.ReferenceColumns(state.all_columns, global_state.projection_ids);
    }

    else {
        output.SetCardinality(output_size);

        ArrowToDuckDB( state
                      ,data.arrow_table.GetColumns()
                      ,output
                      ,data.lines_read - output_size
                      ,was_proj_pusheddown);
    }

    output.Verify();
    state.chunk_offset += output.size();
  }

  TableFunction ArrowIPCTableFunction::GetFunction() {
    // Fields for each IPC buffer to scan
    child_list_t<LogicalType> ipc_buffer_fields {
       {"ptr" , LogicalType::UBIGINT}
      ,{"size", LogicalType::UBIGINT}
    };

    // The function argument is list<struct>; each struct describes an IPC buffer.
    TableFunction scan_arrow_ipc_func(
       "scan_arrow_ipc"
      ,{ LogicalType::LIST(LogicalType::STRUCT(ipc_buffer_fields)) }
      ,ArrowIPCTableFunction::ArrowScanFunction
      ,ArrowIPCTableFunction::ArrowScanBind
      ,ArrowTableFunction::ArrowScanInitGlobal
      ,ArrowTableFunction::ArrowScanInitLocal
    );

    scan_arrow_ipc_func.cardinality = ArrowTableFunction::ArrowScanCardinality;
    // TODO: implement
    scan_arrow_ipc_func.get_batch_index     = ArrowTableFunction::ArrowGetBatchIndex;
    scan_arrow_ipc_func.projection_pushdown = true;
    scan_arrow_ipc_func.filter_pushdown     = false;

    return scan_arrow_ipc_func;
  }

} // namespace duckdb
