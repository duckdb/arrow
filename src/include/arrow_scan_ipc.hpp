#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "arrow_stream_buffer.hpp"

#include "duckdb.hpp"

using ArrowIPCDec = arrow::ipc::StreamDecoder;

namespace duckdb {

  //! Data wrapper for Arrow IPC Scan function data
  struct ArrowIPCScanFunctionData : public ArrowScanFunctionData {
    // Reuse the constructor of parent class
    using ArrowScanFunctionData::ArrowScanFunctionData;

    // A delegate constructor that takes an IPC buffer
    ArrowIPCScanFunctionData( stream_factory_produce_t              reader_factory
                             ,std::shared_ptr<ArrowIPCStreamBuffer> arrow_buffer)
      : ArrowScanFunctionData(reader_factory, (uintptr_t) &arrow_buffer) {
      ipc_buffer = arrow_buffer;
    }

    // An attribute to keep the decoded arrow data alive
    std::shared_ptr<ArrowIPCStreamBuffer> ipc_buffer { nullptr };
  };

  /**
   * ArrowIPCTableScan receives pointers to IPC buffers and returns a DuckDB Table.
   * This implementation is logically identical to ArrowTableFunction arrow scan
   * (used by PyRelation) which takes CDataInterface header pointers instead.
   */
  struct ArrowIPCTableFunction : public ArrowTableFunction {
    public:
      //! Returns a constructed instance of ArrowIPCTableFunction
      static TableFunction GetFunction();

    private:
      static unique_ptr<FunctionData>
      ArrowScanBind( ClientContext&          context
                    ,TableFunctionBindInput& input
                    ,vector<LogicalType>&    return_types
                    ,vector<string>&         names);

      static void
      ArrowScanFunction( ClientContext&      context
                        ,TableFunctionInput& data_p
                        ,DataChunk&          output);
  };

} // namespace duckdb
