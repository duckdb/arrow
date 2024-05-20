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

#include "arrow_stream_buffer.hpp"


// ------------------------------
// Aliases



// ------------------------------
// Public prototypes

// >> Reader functions
namespace duckdb {
  arrow::Result<std::shared_ptr<arrow::Buffer>>
  BufferFromIPCStream(const std::string& path_to_file);

  /*
  Result<shared_ptr<arrow::Table>>
  ReadIPCStream(const std::string& path_to_file);

  Result<shared_ptr<arrow::Table>>
  ReadIPCFile  (const std::string& path_to_file);
  */

  arrow::Status
  ConsumeArrowStream( std::shared_ptr<ArrowIPCStreamBuffer> ipc_buffer
                     ,vector<Value>                         input_buffers);

  Value ValueForIPCBuffer(arrow::Buffer& ipc_buffer);
}
