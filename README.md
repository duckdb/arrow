# Deprecation Note
Starting with DuckDB `v1.3`, this extension will be archived, and its development and distribution will be discontinued. As a replacement, the [nanoarrow extension](https://github.com/paleolimbot/duckdb-nanoarrow) has been developed as a community-driven project, with shared ownership between DuckDB Labs and the Arrow community. The new extension provides the same functionality as the current one, with the added benefit of being able to scan Arrow IPC stream files as well.

For compatibility reasons, the new extension is also aliased to `arrow`, meaning you can install and load it with:
```sql
INSTALL arrow from community;

LOAD arrow;
```

# DuckDB Arrow Extension
This is a [DuckDB](https://www.duckdb.org) extension that provides features that need a dependency on the Apache Arrow library.


## Features
| function | type | description
| --- | --- | --- |
| `to_arrow_ipc` | Table in-out-function | Serializes a table into a stream of blobs containing arrow ipc buffers  
| `scan_arrow_ipc` | Table function | scan a list of pointers pointing to arrow ipc buffers