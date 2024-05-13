#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "arrow_stream_buffer.hpp"

#include "duckdb.hpp"

namespace duckdb {

struct ArrowIPCScanFunctionData : public ArrowScanFunctionData {
public:
  using ArrowScanFunctionData::ArrowScanFunctionData;
  unique_ptr<BufferingArrowIPCStreamDecoder> stream_decoder = nullptr;
};

// IPC Table scan is identical to ArrowTableFunction arrow scan except instead
// of CDataInterface header pointers, it takes a bunch of pointers pointing to
// buffers containing data in Arrow IPC format
struct ArrowIPCTableFunction : public ArrowTableFunction {
public:
  static TableFunction GetFunction();

private:
  static unique_ptr<FunctionData>
  ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names);
  static void ArrowScanFunction(ClientContext &context,
                                TableFunctionInput &data_p, DataChunk &output);
};

} // namespace duckdb
