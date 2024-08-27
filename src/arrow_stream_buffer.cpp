#include "arrow_stream_buffer.hpp"

#include <iostream>
#include <memory>


namespace duckdb {

  // >> ArrowIPCStreamBuffer implementations

  ArrowIPCStreamBuffer::ArrowIPCStreamBuffer()
    : schema_(nullptr), batches_(), is_eos_(false) {}

  //! Event handlers (arrow::ipc::Listener interface)
  arrow::Status
  ArrowIPCStreamBuffer::OnSchemaDecoded(std::shared_ptr<arrow::Schema> s) {
    schema_ = s;
    return arrow::Status::OK();
  }

  arrow::Status
  ArrowIPCStreamBuffer::OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> batch) {
    batches_.push_back(std::move(batch));
    return arrow::Status::OK();
  }

  arrow::Status
  ArrowIPCStreamBuffer::OnEOS() {
    is_eos_ = true;
    return arrow::Status::OK();
  }


  // >> ArrowRecordBatchReader implementations

  ArrowRecordBatchReader::ArrowRecordBatchReader(std::shared_ptr<ArrowIPCStreamBuffer> buffer)
      : buffer_(buffer), next_batch_id_(0) {}

  std::shared_ptr<arrow::Schema>
  ArrowRecordBatchReader::schema() const { return buffer_->schema(); }

  //! Read the next record batch in the stream. Sets *batch to null at end of stream
  arrow::Status
  ArrowRecordBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (next_batch_id_ >= buffer_->batches().size()) {
      *batch = nullptr;
    }

    else {
      *batch = buffer_->batches()[next_batch_id_++];
    }

    return arrow::Status::OK();
  }

  //! Creates a C stream on the input IPC buffer
  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  CStreamForIPCBuffer(uintptr_t buffer_ptr, ArrowStreamParameters &parameters) {
    assert(buffer_ptr != 0);

    auto buffer = reinterpret_cast<std::shared_ptr<ArrowIPCStreamBuffer>*>(buffer_ptr);
    auto reader = std::make_shared<ArrowRecordBatchReader>(*buffer);

    // Create arrow stream
    auto stream_wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream.release = nullptr;

    // Export the RecordBatchReader to use the C data stream interface
    auto export_status = arrow::ExportRecordBatchReader(
      reader, &stream_wrapper->arrow_array_stream
    );

    if (!export_status.ok()) {
      std::cerr << "Error when exporting ArrowRecordBatchReader:" << std::endl
                << export_status.message()                        << std::endl
      ;
      return nullptr;
    }

    return stream_wrapper;
  }

  //! Sets Arrow schema (C data interface) from input IPC buffer
  void SchemaFromIPCBuffer( std::shared_ptr<ArrowIPCStreamBuffer> buffer
                           ,duckdb::ArrowSchemaWrapper&      schema) {
    auto reader = std::make_shared<ArrowRecordBatchReader>(buffer);

    // Export RecordBatchReader to C data interface so we get an `ArrowSchema`
    auto stream_wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream.release = nullptr;

    auto export_status = arrow::ExportRecordBatchReader(
      reader, &stream_wrapper->arrow_array_stream
    );

    if (!export_status.ok()) { return; }

    // Set the `arrow_schema` attribute of ArrowSchemaWrapper (also passes ownership)
    stream_wrapper->arrow_array_stream.get_schema(
       &stream_wrapper->arrow_array_stream
      ,&schema.arrow_schema
    );
  }

} // namespace duckdb
