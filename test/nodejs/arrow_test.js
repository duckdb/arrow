var duckdb = require('../../duckdb/tools/nodejs');
var arrow = require('../../duckdb/tools/nodejs/node_modules/apache-arrow')
var assert = require('assert');

const parquet_file_path = "../../../data/parquet-testing/lineitem_sf0_01.parquet";

// Wrapper for tests, materializes whole stream
const arrow_ipc_stream = async (conn, sql) => {
    const result_stream = await conn.arrowIPCStream(sql);
    return await result_stream.toArray();
}

// Wrapper for tests
const arrow_ipc_materialized = async (conn, sql) => {
    return await new Promise((resolve, reject) => {
        conn.arrowIPCAll(sql, function (err, result) {
            if (err) {
                reject(err)
            }

            resolve(result);
        })
    });
}

const to_ipc_functions = {
    'streaming': arrow_ipc_stream,
    'materialized': arrow_ipc_materialized,
}

function getDatabase() {
    return new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"});
}

function getConnection(db, done) {
    let conn = new duckdb.Connection(db);
    conn.exec(`LOAD '${process.env.ARROW_EXTENSION_BINARY_PATH}';`, function (err) {
        if (err) throw err;
        done();
    });
    return conn
}

describe(`Arrow IPC`, () => {
    let db;
    let conn;
    before((done) => {
        db = getDatabase();
        conn = getConnection(db, () => done())
    });

    it(`Basic examples`, async () => {
        const range_size = 130000;
        const query = `SELECT * FROM range(0,${range_size}) tbl(i)`;
        const arrow_table_expected = new arrow.Table({
            i: new arrow.Vector([arrow.makeData({ type: new arrow.Int32, data: Array.from(new Array(range_size), (x, i) => i) })]),
        });

        // Can use Arrow to read from stream directly
        const result_stream = await db.arrowIPCStream(query);
        const reader = await arrow.RecordBatchReader.from(result_stream);
        const table = await arrow.tableFromIPC(reader);
        const array_from_arrow = table.toArray();
        assert.deepEqual(array_from_arrow, arrow_table_expected.toArray());

        // Can also fully materialize stream first, then pass to Arrow
        const result_stream2 = await db.arrowIPCStream(query);
        const reader2 = await arrow.RecordBatchReader.from(result_stream2.toArray());
        const table2 = await arrow.tableFromIPC(reader2);
        const array_from_arrow2 = table2.toArray();
        assert.deepEqual(array_from_arrow2, arrow_table_expected.toArray());

        // Can also fully materialize in DuckDB first (allowing parallel execution)
        const result_materialized = await new Promise((resolve, reject) => {
            db.arrowIPCAll(query, function (err, result) {
                if (err) {
                    reject(err)
                }

                resolve(result);
            })
        });

        const reader3 = await arrow.RecordBatchReader.from(result_materialized);
        const table3 = await arrow.tableFromIPC(reader3);
        const array_from_arrow3 = table3.toArray();
        assert.deepEqual(array_from_arrow3, arrow_table_expected.toArray());

        // Scanning materialized IPC buffers from DuckDB
        db.register_buffer("ipc_table", result_materialized, true);
        await new Promise((resolve, reject) => {
            db.arrowIPCAll(`SELECT * FROM ipc_table`, function (err, result) {
                if (err) {
                    reject(err);
                }

                assert.deepEqual(result, result_materialized);
                resolve()
            });
        });
    });

    // Ensure we handle empty result properly
    for (const [name, fun] of Object.entries(to_ipc_functions)) {
        it(`Empty results (${name})`, async () => {
            const range_size = 130000;
            const query = `SELECT * FROM range(0,${range_size}) tbl(i) where i > ${range_size}`;

            let ipc_buffers = await fun(conn, query);
            const reader = await arrow.RecordBatchReader.from(ipc_buffers);
            const table = await arrow.tableFromIPC(reader);
            const arr = table.toArray();
            assert.deepEqual(arr, []);
        });
    }
})

for (const [name, fun] of Object.entries(to_ipc_functions)) {
    describe(`DuckDB <-> Arrow IPC (${name})`, () => {
        const total = 1000;

        let db;
        let conn;
        before((done) => {
            db = getDatabase();
            conn = getConnection(db, () => done())
        });

        it(`Buffers are not garbage collected`, async () => {
            let ipc_buffers = await fun(conn, 'SELECT * FROM range(1001, 2001) tbl(i)');

            // Now to scan the buffer, we first need to register it
            conn.register_buffer(`ipc_table_${name}`, ipc_buffers, true);

            // Delete JS reference to arrays
            ipc_buffers = 0;

            // Run GC to ensure file is deleted
            if (global.gc) {
                global.gc();
            } else {
                throw "should run with --expose-gc";
            }

            // Spray memory overwriting hopefully old buffer
            let spray_results = [];
            for (let i = 0; i < 3000; i++) {
                spray_results.push(await fun(db, 'SELECT * FROM range(2001, 3001) tbl(i)'));
            }

            // Now we can query the ipc buffer using DuckDB by providing an object with an alias and the materialized ipc buffers
            await new Promise((resolve, reject) => {
                conn.all(`SELECT avg(i) as average, count(1) as total
                        FROM ipc_table_${name};`, function (err, result) {
                    if (err) {
                        reject(err);
                    }
                    assert.deepEqual(result, [{average: 1500.5, total: 1000}]);
                    resolve();
                });
            });
        });

        it(`Round-trip int column`, async () => {
            // Now we fetch the ipc stream object and construct the RecordBatchReader
            const ipc_buffers = await fun(db, 'SELECT * FROM range(1001, 2001) tbl(i)');

            // Now to scan the buffer, we first need to register it
            conn.register_buffer("ipc_table", ipc_buffers, true, (err) => {
                assert(!err);
            });

            // Now we can query the ipc buffer using DuckDB by providing an object with an alias and the materialized ipc buffers
            await new Promise((resolve, reject) => {
                conn.all(`SELECT avg(i) as average, count(1) as total
                        FROM ipc_table;`, function (err, result) {
                    if (err) {
                        reject(err)
                    }
                    assert.deepEqual(result, [{average: 1500.5, total: 1000}]);
                    resolve();
                });
            });
        });


        it(`Joining 2 IPC buffers in DuckDB`, async () => {
            // Insert first table
            const ipc_buffers1 = await fun(db, 'SELECT * FROM range(1, 3) tbl(i)');

            // Insert second table
            const ipc_buffers2 = await fun(db, 'SELECT * FROM range(2, 4) tbl(i)');

            // Register buffers for scanning from DuckDB
            conn.register_buffer("table1", ipc_buffers1, true, (err) => {
                assert(!err);
            });
            conn.register_buffer("table2", ipc_buffers2, true, (err) => {
                assert(!err);
            });

            await new Promise((resolve, reject) => {
                conn.all(`SELECT *
                        FROM table1
                                 JOIN table2 ON table1.i = table2.i;`, function (err, result) {
                    if (err) {
                        reject(err);
                    }
                    assert.deepEqual(result, [{i: 2}]);
                    resolve()
                });
            });
        });

        it(`Registering multiple IPC buffers for the same table in DuckDB`, async () => {
            const table = await fun(db, 'SELECT * FROM range(1, 10000) tbl(i)');

            db.register_buffer("table1", table, true, (err) => {
                assert(!err);
            });
        });

        it(`Registering IPC buffers for two different tables fails`, async () => {
            const ipc_buffers1 = await fun(db, 'SELECT * FROM range(1, 3) tbl(i)');
            const ipc_buffers2 = await fun(db, 'SELECT * FROM range(2, 4) tbl(i)');

            // This will fail when reading buffers for the second table
            db.register_buffer("table1", [...ipc_buffers1, ...ipc_buffers2], true, (err) => {
                assert(err);
            });
        });
    })
}

describe('[Benchmark] Arrow IPC Single Int Column (50M tuples)',() => {
    // Config
    const column_size = 50*1000*1000;

    let db;
    let conn;

    before((done) => {
        db = getDatabase();
        conn = getConnection(db, () => {
            conn.run("CREATE TABLE test AS select * FROM range(0,?) tbl(i);", column_size, (err) => {
                if (err) throw err;
                done()
            });
        })
    });

    it('DuckDB table -> DuckDB table', (done) => {
        conn.run('CREATE TABLE copy_table AS SELECT * FROM test', (err) => {
            assert(!err);
            done();
        });
    });

    it('DuckDB table -> Stream IPC buffer', async () => {
        const result = await conn.arrowIPCStream('SELECT * FROM test');
        const ipc_buffers = await result.toArray();
        const reader = await arrow.RecordBatchReader.from(ipc_buffers);
        const table = arrow.tableFromIPC(reader);
        assert.equal(table.numRows, column_size);
    });

    it('DuckDB table -> Materialized IPC buffer',  (done) => {
        conn.arrowIPCAll('SELECT * FROM test', (err,res) => {
            done();
        });
    });
});

describe('Buffer registration',() => {
    let db;
    let conn1;
    let conn2;

    before((done) => {
        db = new duckdb.Database(':memory:',  {"allow_unsigned_extensions":"true"});
        conn1 = new duckdb.Connection(db);
        conn2 = new duckdb.Connection(db);
        done();
    });

    before((done) => {
        db = getDatabase();
        conn1 = getConnection(db, () => {
            conn2 = getConnection(db, () => done());
        })
    });

    it('Buffers can only be overwritten with force flag',  async () => {
        const arrow_buffer = await arrow_ipc_materialized(conn1, "SELECT 1337 as a");

        conn1.register_buffer('arrow_buffer', arrow_buffer, true, (err) => {
            assert(!err);
        })

        await new Promise((resolve, reject) => {
            try {
                conn1.register_buffer('arrow_buffer', arrow_buffer, false);
                reject("Expected query to fail");
            } catch (err) {
                assert(err.message.includes('Buffer with this name already exists and force_register is not enabled'));
                resolve();
            }
        });
    });

    it('Existing tables are silently shadowed by registered buffers',  async () => {
        // Unregister, in case other test has registered this
        conn1.unregister_buffer('arrow_buffer', (err) => {
            assert(!err);
        });

        conn1.run('CREATE TABLE arrow_buffer AS SELECT 7 as a;', (err) => {
            assert(!err);
        });

        conn1.all('SELECT * FROM arrow_buffer;', (err, result) => {
            assert(!err);
            assert.deepEqual(result, [{'a': 7}]);
        });

        const arrow_buffer = await arrow_ipc_materialized(conn1, "SELECT 1337 as b");

        conn1.register_buffer('arrow_buffer', arrow_buffer, true, (err) => {
            assert(!err);
        })

        conn1.all('SELECT * FROM arrow_buffer;', (err, result) => {
            assert(!err);
            assert.deepEqual(result, [{'b': 1337}]);
        });

        conn1.unregister_buffer('arrow_buffer', (err) => {
            assert(!err);
        });

        conn1.all('SELECT * FROM arrow_buffer;', (err, result) => {
            assert(!err);
            assert.deepEqual(result, [{'a': 7}]);
        });

        await new Promise((resolve, reject) => {
            // Cleanup
            conn1.run('DROP TABLE arrow_buffer;', (err) => {
                if (err) reject(err);
                resolve();
            });

        });
    });

    it('Registering buffers should only be visible within current connection', async () => {
        const arrow_buffer1 = await arrow_ipc_materialized(conn1, "SELECT 1337 as a");
        const arrow_buffer2 = await arrow_ipc_materialized(conn2, "SELECT 42 as b");

        conn1.register_buffer('arrow_buffer', arrow_buffer1, true, (err) => {
            assert(!err);
        })
        conn2.register_buffer('arrow_buffer', arrow_buffer2, true, (err) => {
            assert(!err);
        })

        conn1.all('SELECT * FROM arrow_buffer;', (err, result) => {
            assert(!err);
            assert.deepEqual(result, [{'a': 1337}]);
        });

        conn2.all('SELECT * FROM arrow_buffer;', (err, result) => {
            assert(!err);
            assert.deepEqual(result, [{'b': 42}]);
        });

        conn1 = 0;

        conn2.all('SELECT * FROM arrow_buffer;', (err, result) => {
            assert(!err);
            assert.deepEqual(result, [{'b': 42}]);
        });

        conn2.unregister_buffer('arrow_buffer', (err) => {
            assert(!err);
        })

        await new Promise((resolve, reject) => {
            conn2.all('SELECT * FROM arrow_buffer;', (err, result) => {
                if (!err) {
                    reject("Expected error");
                }
                assert(err.message.includes('Catalog Error: Table with name arrow_buffer does not exist!'));
                resolve();
            });
        });
    });
});

describe('[Benchmark] Arrow IPC TPC-H lineitem.parquet', () => {
    const sql = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
    const answer = [{revenue: 1193053.2253}];

    let db;
    let conn;

    before((done) => {
        db = getDatabase();
        conn = getConnection(db, () => done())
    });

    it('Parquet -> DuckDB Streaming-> Arrow IPC -> DuckDB Query', async () => {
        const ipc_buffers = await arrow_ipc_stream(conn, 'SELECT * FROM "' + parquet_file_path + '"');

        const query = sql.replace("lineitem", "my_arrow_ipc_stream");
        conn.register_buffer("my_arrow_ipc_stream", ipc_buffers, true, (err) => {
            assert(!err);
        });

        await new Promise((resolve, reject) => {
            conn.all(query, function (err, result) {
                if (err) {
                    reject(err)
                }

                assert.deepEqual(result, answer);
                resolve();
            })
        });
    });

    it('Parquet -> DuckDB Materialized -> Arrow IPC -> DuckDB' , async () => {
        const ipc_buffers = await arrow_ipc_materialized(conn, 'SELECT * FROM "' + parquet_file_path + '"');

        const query = sql.replace("lineitem", "my_arrow_ipc_stream_2");
        conn.register_buffer("my_arrow_ipc_stream_2", ipc_buffers, true, (err) => {
            assert(!err);
        });

        await new Promise((resolve, reject) => {
            conn.all(query, function (err, result) {
                if (err) {
                    reject(err)
                } else {
                    assert.deepEqual(result, answer);
                    resolve();
                }
            })
        });
    });

    it('Parquet -> DuckDB', async () => {
        await new Promise((resolve, reject) => {
            conn.run('CREATE TABLE load_parquet_directly AS SELECT * FROM "' + parquet_file_path + '";', (err) => {
                if (err) {
                    reject(err)
                }
                resolve()
            });
        });

        const query = sql.replace("lineitem", "load_parquet_directly");

        const result = await new Promise((resolve, reject) => {
            conn.all(query, function (err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result)
            });
        });

        assert.deepEqual(result, answer);
    });
});

for (const [name, fun] of Object.entries(to_ipc_functions)) {
    describe(`Arrow IPC TPC-H lineitem SF0.01 (${name})`, () => {
        // `table_name` in these queries will be replaced by either the parquet file directly, or the ipc buffer
        const queries = [
            "select count(*) from table_name LIMIT 10",
            "select sum(l_orderkey) as sum_orderkey FROM table_name",
            "select * from table_name",
            "select l_orderkey from table_name WHERE l_orderkey=2 LIMIT 2",
            "select l_extendedprice from table_name",
            "select l_extendedprice from table_name WHERE l_extendedprice > 53468 and l_extendedprice < 53469  LIMIT 2",
            "select count(l_orderkey) from table_name where l_commitdate > '1996-10-28'",
            "SELECT sum(l_extendedprice * l_discount) AS revenue FROM table_name WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
        ];

        let db;
        let conn;
        before((done) => {
            db = getDatabase();
            conn = getConnection(db, () => done())
        });

        for (const query of queries) {
            it(` ${query}`, async () => {
                // First do query directly on parquet file
                const expected_value = await new Promise((resolve, reject) => {
                    conn.all(query.replace("table_name", `'${parquet_file_path}'`), function (err, result) {
                        if (err) {
                            reject(err);
                        }

                        resolve(result);
                    });
                });

                // Copy parquet file completely into Arrow IPC format
                const ipc_buffers = await fun(conn, 'SELECT * FROM "' + parquet_file_path + '"');

                // Register the ipc buffers as table in duckdb, using force to override the previously registered buffers
                conn.register_buffer("table_name", ipc_buffers, true, (err) => {
                    assert(!err);
                });

                await new Promise((resolve, reject) => {
                    conn.all(query, function (err, result) {
                        if (err) {
                            reject(err)
                        }

                        assert.deepEqual(result, expected_value, `Query failed: ${query}`);
                        resolve();
                    })
                });
            });
        }
    })
}


