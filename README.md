# DuckDB Arrow Extension
This is a [DuckDB](https://www.duckdb.org) extension that provides features that need a dependency on the Apache Arrow library.


## Features
| function | type | description
| --- | --- | --- |
| `to_arrow_ipc` | Table in-out-function | Serializes a table into a stream of blobs containing arrow ipc buffers  
| `scan_arrow_ipc` | Table function | scan a list of pointers pointing to arrow ipc buffers



