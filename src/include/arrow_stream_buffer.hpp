#pragma once

#include <string>
#include <unordered_map>
#include <iostream>
#include <memory>

// >> DuckDB dependencies
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/table_filter.hpp"

// >> Arrow dependencies
#include "arrow/type_fwd.h"
#include "arrow/type.h"
#include "arrow/status.h"
#include "arrow/result.h"
#include "arrow/buffer.h"
#include "arrow/record_batch.h"

#include "arrow/io/file.h"
#include "arrow/ipc/reader.h"
#include "arrow/c/bridge.h"


// ------------------------------
// Convenience Aliases

using ArrowIPCDec = arrow::ipc::StreamDecoder;


// >> Custom parsing of Arrow IPC

namespace duckdb {

  //! An IPC Listener that stores the schema and batches of an IPC stream
  struct ArrowIPCStreamBuffer : public arrow::ipc::Listener {
      ArrowIPCStreamBuffer();

      bool                            is_eos()  const { return is_eos_;  }
      std::shared_ptr<arrow::Schema>& schema()        { return schema_;  }
      arrow::RecordBatchVector&       batches()       { return batches_; }

    protected:
      std::shared_ptr<arrow::Schema> schema_;
      arrow::RecordBatchVector       batches_;
      bool is_eos_;

      arrow::Status      OnSchemaDecoded(std::shared_ptr<arrow::Schema>      schema      );
      arrow::Status OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> record_batch);
      arrow::Status OnEOS();
  };


  struct ArrowRecordBatchReader : public arrow::RecordBatchReader {
      ArrowRecordBatchReader(std::shared_ptr<ArrowIPCStreamBuffer> buffer);

      ~ArrowRecordBatchReader() = default;

      std::shared_ptr<arrow::Schema> schema() const override;

      //! Read the next record batch in the stream. Returns nullptr at the end.
      arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override;

    protected:
      std::shared_ptr<ArrowIPCStreamBuffer> buffer_;
      size_t                                next_batch_id_;
  };


  //! Exports an ArrowRecordBatchReader on the input IPC buffer to the C data interface
  duckdb::unique_ptr<ArrowArrayStreamWrapper>
  CStreamForIPCBuffer(uintptr_t buffer_ptr, ArrowStreamParameters &parameters);

  //! Uses an ArrowRecordBatchReader on the input IPC buffer to set schema
  void SchemaFromIPCBuffer( std::shared_ptr<ArrowIPCStreamBuffer> buffer
                           ,ArrowSchemaWrapper&                   schema);

} // namespace duckdb
