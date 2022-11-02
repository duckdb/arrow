#define DUCKDB_EXTENSION_MAIN

#include "arrow_extension.hpp"
#include "arrow_stream_buffer.hpp"
#include "arrow_scan_ipc.hpp"
#include "arrow_to_ipc.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table/arrow.hpp"
#endif

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

    auto to_arrow_fun = ToArrowIPCFunction::GetFunction();
	CreateTableFunctionInfo to_arrow_ipc_info(to_arrow_fun);
	catalog.CreateTableFunction(*con.context, &to_arrow_ipc_info);

    auto scan_arrow_fun = ArrowIPCTableFunction::GetFunction();
	CreateTableFunctionInfo scan_arrow_ipc_info(scan_arrow_fun);
	catalog.CreateTableFunction(*con.context, &scan_arrow_ipc_info);

	con.Commit();
}

void ArrowExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ArrowExtension::Name() {
	return "arrow";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void arrow_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *arrow_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
