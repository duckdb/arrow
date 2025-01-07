#pragma once

#include "arrow/buffer.h"
#include "duckdb.hpp"


namespace duckdb {

  class ArrowStringVectorBuffer : public VectorBuffer {
    public:
      explicit ArrowStringVectorBuffer(std::shared_ptr <arrow::Buffer> buffer_p)
        :  VectorBuffer(VectorBufferType::OPAQUE_BUFFER)
          ,buffer      (std::move(buffer_p)            ) {}

    private:
      std::shared_ptr<arrow::Buffer> buffer;
  };


  class ToArrowIPCFunction {
    public:
      //! note: this is the number of vectors per chunk
      static constexpr idx_t DEFAULT_CHUNK_SIZE = 120;

      static TableFunction GetFunction();

    private:
      static unique_ptr<LocalTableFunctionState>
      InitLocal( ExecutionContext&         context
                ,TableFunctionInitInput&   input
                ,GlobalTableFunctionState* global_state);

      static unique_ptr<GlobalTableFunctionState>
      InitGlobal(ClientContext& context, TableFunctionInitInput& input);

      static unique_ptr<FunctionData>
      Bind( ClientContext&          context
           ,TableFunctionBindInput& input
           ,vector<LogicalType>&    return_types
           ,vector<string>&         names);

      static OperatorResultType
      Function( ExecutionContext&   context
               ,TableFunctionInput& data_p
               ,DataChunk&          input
               ,DataChunk&          output);

      static OperatorFinalizeResultType
      FunctionFinal( ExecutionContext&   context
                    ,TableFunctionInput& data_p
                    ,DataChunk&          output);

  };

} // namespace duckdb
