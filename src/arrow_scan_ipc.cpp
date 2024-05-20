#include "arrow_scan_ipc.hpp"

namespace duckdb {

  unique_ptr<FunctionData>
  ArrowIPCTableFunction::BindFnScanArrowIPC( ClientContext&          context
                                            ,TableFunctionBindInput& input
                                            ,vector <LogicalType>&   return_types
                                            ,vector <string>&        names) {
    // Extract function arguments
    auto input_buffers = ListValue::GetChildren(input.inputs[0]);

    // Maintain the Arrow data (IPC buffer) and a function to create a RecordBatchReader
    auto ipc_buffer     = std::make_shared<ArrowIPCStreamBuffer>();
    auto reader_factory = (stream_factory_produce_t) &CStreamForIPCBuffer;
    auto fn_data        = make_uniq<ArrowIPCScanFunctionData>(reader_factory, ipc_buffer);

    // TODO: Can we reduce this to only read the schema at binding time?
    // Start by reading all data from input IPC buffers
    auto decode_status = ConsumeArrowStream(ipc_buffer, input_buffers);

    // Errors if status is not OK or if the decoder never saw an end-of-stream marker
    if (!decode_status.ok())   { throw IOException("Invalid IPC stream");    }
    if (!ipc_buffer->is_eos()) { throw IOException("Incomplete IPC stream"); }

    // First, decode the schema from the IPC stream into `fn_data`
    // Then, bind the logical schema types (`names` and `return_types`)
    SchemaFromIPCBuffer(ipc_buffer, fn_data->schema_root);
    PopulateArrowTableType(
       fn_data->arrow_table
      ,fn_data->schema_root
      ,names
      ,return_types
    );

    QueryResult::DeduplicateColumns(names);
    return std::move(fn_data);
  }

  unique_ptr<FunctionData>
  ArrowIPCTableFunction::BindFnScanArrows( ClientContext&          context
                                          ,TableFunctionBindInput& input
                                          ,vector <LogicalType>&   return_types
                                          ,vector <string>&        names) {
    // TODO: Only get the schema at binding time; decode the buffers later
    // Extract function arguments
    auto input_files = ListValue::GetChildren(input.inputs[0]);

    // Maintain the Arrow data (IPC buffer) and a function to create a RecordBatchReader
    auto ipc_buffer     = std::make_shared<ArrowIPCStreamBuffer>();
    auto reader_factory = (stream_factory_produce_t) &CStreamForIPCBuffer;
    auto fn_data        = make_uniq<ArrowIPCScanFunctionData>(reader_factory, ipc_buffer);

    // Read IPC buffers from files
    vector<Value> duckdb_buffers;
    for (auto& file_uri : input_files) {
      string uri_str = file_uri.GetValue<string>();

      // NOTE: not thread safe (I don't think)
      // If we have seen this file before, assume it has been read already
      const auto& map_entry = fn_data->file_opened.find(uri_str);
      if (map_entry != fn_data->file_opened.end()) { continue; }

      // Read the buffer from the file
      auto result_buffer = BufferFromIPCStream(uri_str);
      if (not result_buffer.ok()) {
        throw IOException("Unable to parse file '" + uri_str + "'");
      }
      std::shared_ptr<arrow::Buffer> file_buffer = result_buffer.ValueOrDie();

      duckdb_buffers.push_back(ValueForIPCBuffer(*file_buffer));
      fn_data->file_buffers.push_back(std::move(file_buffer));
      fn_data->file_opened[uri_str] = true;
    }

    // Start by reading all data from input IPC buffers
    auto src_data_buffer = ListValue::GetChildren(Value::LIST(duckdb_buffers));
    auto decode_status   = ConsumeArrowStream(ipc_buffer, src_data_buffer);

    // Errors if status is not OK or if the decoder never saw an end-of-stream marker
    if (!decode_status.ok())   { throw IOException("Invalid IPC stream");    }
    if (!ipc_buffer->is_eos()) { throw IOException("Incomplete IPC stream"); }

    // First, decode the schema from the IPC stream into `fn_data`
    // Then, bind the logical schema types (`names` and `return_types`)
    SchemaFromIPCBuffer(ipc_buffer, fn_data->schema_root);
    PopulateArrowTableType(
       fn_data->arrow_table
      ,fn_data->schema_root
      ,names
      ,return_types
    );

    QueryResult::DeduplicateColumns(names);
    return std::move(fn_data);
  }

  // TODO: refactor to allow nicely overriding this
  //! Scan implementation for Arrow data. Similar logic as python API, except ArrowToDuckDB call
  void ArrowIPCTableFunction::TableFnScanArrowIPC( ClientContext&      context
                                                  ,TableFunctionInput& data_p
                                                  ,DataChunk&          output) {
    if (!data_p.local_state) { return; }

    auto &data         = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
    auto &state        = data_p.local_state->Cast<ArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

    // Check if we are out of tuples in this chunk
    if (state.chunk_offset >= (idx_t) state.chunk->arrow_array.length) {

      // Check if there is a next chunk
      bool has_next_chunk = ArrowScanParallelStateNext(
        context, data_p.bind_data.get(), state, global_state
      );

      if (!has_next_chunk) { return; }
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

  TableFunction ArrowIPCTableFunction::GetFunctionScanIPC() {
    // Fields for each IPC buffer to scan
    child_list_t<LogicalType> ipc_buffer_fields {
       {"ptr" , LogicalType::UBIGINT}
      ,{"size", LogicalType::UBIGINT}
    };

    // The function argument is list<struct>; each struct describes an IPC buffer.
    TableFunction scan_arrow_ipc_func(
       "scan_arrow_ipc"
      ,{ LogicalType::LIST(LogicalType::STRUCT(ipc_buffer_fields)) }
      ,ArrowIPCTableFunction::TableFnScanArrowIPC
      ,ArrowIPCTableFunction::BindFnScanArrowIPC
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

  TableFunction ArrowIPCTableFunction::GetFunctionScanArrows() {
    // The function argument is list<varchar>; each string is a file URI.
    TableFunction tfn_scan_arrows(
       "scan_arrows_file"
      ,{ LogicalType::LIST(LogicalType::VARCHAR) }
      ,ArrowIPCTableFunction::TableFnScanArrowIPC
      // ,ArrowIPCTableFunction::TableFnScanArrows
      ,ArrowIPCTableFunction::BindFnScanArrows
      ,ArrowTableFunction::ArrowScanInitGlobal
      ,ArrowTableFunction::ArrowScanInitLocal
    );

    tfn_scan_arrows.cardinality = ArrowTableFunction::ArrowScanCardinality;
    // TODO: implement
    tfn_scan_arrows.get_batch_index     = ArrowTableFunction::ArrowGetBatchIndex;
    tfn_scan_arrows.projection_pushdown = true;
    tfn_scan_arrows.filter_pushdown     = false;

    return tfn_scan_arrows;
  }

} // namespace duckdb
