#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "arrow_readers.hpp"

#include "duckdb.hpp"


namespace duckdb {

  //! Data wrapper for Arrow IPC Scan function data
  struct ArrowIPCScanFunctionData : public ArrowScanFunctionData {
    // Reuse the constructor of parent class
    using ArrowScanFunctionData::ArrowScanFunctionData;

    // A delegate constructor that takes an IPC buffer
    ArrowIPCScanFunctionData( stream_factory_produce_t              reader_factory
                             ,std::shared_ptr<ArrowIPCStreamBuffer> arrow_buffer)
      : ArrowScanFunctionData(reader_factory, 0ULL) {
      ipc_buffer         = std::move(arrow_buffer);
      stream_factory_ptr = (uintptr_t) &ipc_buffer;
    }

    // An attribute to keep the decoded arrow data alive
    std::shared_ptr<ArrowIPCStreamBuffer>       ipc_buffer { nullptr };
    std::unordered_map<std::string, bool>       file_opened;
    std::vector<std::shared_ptr<arrow::Buffer>> file_buffers;
  };

  /**
   * ArrowIPCTableScan receives pointers to IPC buffers and returns a DuckDB Table.
   * This implementation is logically identical to ArrowTableFunction arrow scan
   * (used by PyRelation) which takes CDataInterface header pointers instead.
   */
  struct ArrowIPCTableFunction : public ArrowTableFunction {
    public:
      //! Return a TableFunction that scans IPC buffers directly
      static TableFunction GetFunctionScanIPC();

      //! Return a TableFunction that scans IPC buffers from a file
      static TableFunction GetFunctionScanArrows();

    private:

      // >> Supporting functions for `scan_arrow_ipc`

      //! Binding for arrow IPC buffers
      static unique_ptr<FunctionData>
      BindFnScanArrowIPC( ClientContext&          context
                         ,TableFunctionBindInput& input
                         ,vector<LogicalType>&    return_types
                         ,vector<string>&         names);

      //! Scan implementation for arrow IPC buffers
      static void
      TableFnScanArrowIPC( ClientContext&      context
                          ,TableFunctionInput& data_p
                          ,DataChunk&          output);

      // >> Supporting functions for `scan_arrows_file`

      //! Binding for arrow IPC buffers
      static unique_ptr<FunctionData>
      BindFnScanArrows( ClientContext&          context
                       ,TableFunctionBindInput& input
                       ,vector<LogicalType>&    return_types
                       ,vector<string>&         names);

      //! Scan implementation for arrow IPC buffers
      static void
      TableFnScanArrows( ClientContext&      context
                        ,TableFunctionInput& data_p
                        ,DataChunk&          output);
  };

} // namespace duckdb
