// ------------------------------
// License
//
// Copyright 2024 Aldrin Montana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


// ------------------------------
// Dependencies

#include "arrow_readers.hpp"


// ------------------------------
// Reader implementations
// TODO: eventually migrate these to prefer internal duckdb types

namespace duckdb {

  // Anonymous namespace for internal functions
  namespace {

    /** Given a file path, return an arrow::ReadableFile. */
    arrow::Result<std::shared_ptr<arrow::io::ReadableFile>>
    HandleForIPCFile(const std::string &path_as_uri) {
      std::cout << "Creating handle for arrow IPC-formatted file: "
                << path_as_uri
                << std::endl
      ;

      // use the `FileSystem` instance to open a handle to the file
      return arrow::io::ReadableFile::Open(path_as_uri);
    }

    /** Given a file path, create a RecordBatchStreamReader. */
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>>
    ReaderForIPCStream(const std::string &path_as_uri) {
      std::cout << "Creating reader for IPC stream" << std::endl;

      // use the `FileSystem` instance to open a handle to the file
      ARROW_ASSIGN_OR_RAISE(auto input_file_handle, HandleForIPCFile(path_as_uri));

      // read from the handle using `RecordBatchStreamReader`
      return arrow::ipc::RecordBatchStreamReader::Open(input_file_handle);
    }

    /** Given a file path, create a RecordBatchFileReader. */
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchFileReader>>
    ReaderForIPCFile(const std::string &path_as_uri) {
      std::cout << "Creating reader for IPC file" << std::endl;

      // use the `FileSystem` instance to open a handle to the file
      ARROW_ASSIGN_OR_RAISE(auto input_file_handle, HandleForIPCFile(path_as_uri));

      // read from the handle using `RecordBatchStreamReader`
      return arrow::ipc::RecordBatchFileReader::Open(input_file_handle);
    }

  } // anonymous namespace: mohair::<anonymous>


  // >> Implementations that wrap internal functions from the anonymous namespace
  //! Given a file path to an Arrow IPC stream, return the data as a buffer.
  arrow::Result<std::shared_ptr<arrow::Buffer>>
  BufferFromIPCStream(const std::string& path_to_file) {
    // use the `FileSystem` instance to open a handle to the file
    ARROW_ASSIGN_OR_RAISE(auto arrow_fhandle, HandleForIPCFile(path_to_file));

    // get the size of the file handle (arrow::io::RandomAccessFile) and read it whole
    ARROW_ASSIGN_OR_RAISE(auto  arrow_fsize, arrow_fhandle->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto arrow_buffer, arrow_fhandle->ReadAt(0, arrow_fsize));

    if (not arrow_buffer->is_cpu()) {
      return arrow::Status::Invalid(
        "Read IPC stream file into memory but it is not CPU-accessible"
      );
    }

    return arrow_buffer;
  }

  /*
  //! Given a file path to an Arrow IPC stream, return a Table.
  arrow::Result<std::shared_ptr<Table>>
  ReadIPCStream(const std::string& path_to_file) {
    // Declares and initializes `batch_reader`
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, ReaderForIPCStream(path_to_file));

    return arrow::Table::FromRecordBatchReader(batch_reader.get());
  }

  //! Given a file path to an Arrow IPC file, return a Table.
  arrow::Result<std::shared_ptr<Table>>
  ReadIPCFile(const std::string& path_to_file) {
    // Declares and initializes `ipc_file_reader`
    ARROW_ASSIGN_OR_RAISE(auto ipc_file_reader, ReaderForIPCFile(path_to_file));

    // Based on RecordBatchFileReader::ToTable (Arrow >12.0.1)
    // https://github.com/apache/arrow/blob/main/cpp/src/arrow/ipc/reader.h#L236-L237
    RecordBatchVector batches;

    const auto batch_count = ipc_file_reader->num_record_batches();
    for (int batch_ndx = 0; batch_ndx < batch_count; ++batch_ndx) {
      ARROW_ASSIGN_OR_RAISE(auto batch, ipc_file_reader->ReadRecordBatch(batch_ndx));
      batches.emplace_back(batch);
    }

    return arrow::Table::FromRecordBatches(ipc_file_reader->schema(), batches);
  }
  */

  // >> Implementations that prefer internal duckdb types

  //! Decode data from `duckdb_bufferlist` into `ipc_buffer` using arrow::ipc::Decoder.
  arrow::Status
  ConsumeArrowStream( std::shared_ptr<ArrowIPCStreamBuffer> ipc_buffer
                     ,vector<duckdb::Value> duckdb_bufferlist) {
    // Decoder consumes each IPC buffer and the `ipc_buffer` listens for its events
    auto stream_decoder = make_uniq<ArrowIPCDec>(std::move(ipc_buffer));

    for (auto& duckdb_buffer : duckdb_bufferlist) {
      // Each "buffer" is a pointer to data and a buffer size
      auto     buffer_struct = StructValue::GetChildren(duckdb_buffer);
      uint64_t buffer_ptr    = buffer_struct[0].GetValue<uint64_t>();
      uint64_t buffer_size   = buffer_struct[1].GetValue<uint64_t>();

      auto decode_status = stream_decoder->Consume((const uint8_t*) buffer_ptr, buffer_size);
      if (!decode_status.ok()) { return decode_status; }
    }

    return arrow::Status::OK();
  }


  //! Given an `arrow::Buffer`, return a pointer to the buffer and its size wrapped in a
  //  duckdb value. The value structure is:
  //    vector<std::pair<std::string, duckdb::Value>>
  Value ValueForIPCBuffer(arrow::Buffer& ipc_buffer) {
    // Place values into a child_list_t<type>
    child_list_t<Value> struct_vals {
       { "ptr" , Value::UBIGINT((uintptr_t) ipc_buffer.mutable_data()) }
      ,{ "size", Value::UBIGINT((uint64_t)  ipc_buffer.size())         }
    };

    // Call the STRUCT builder function
    return Value::STRUCT(struct_vals);
  }

} // namespace: duckdb
