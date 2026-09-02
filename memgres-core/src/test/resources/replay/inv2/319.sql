-- source: investigation-2026-08.md
-- finding: 319
-- title: The result rows and the result column type are assembled from the text lines after the fact: COSTS OFF joins every line into one row, the JSON/XML/YAML branches
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_s" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_s WHERE v = 1;
-- begin-expected
-- columns: QUERY PLAN:json
-- row: [\n  {\n    "Plan": {\n      "Node Type": "Result",\n      "Parallel Aware": false,\n      "Async Capable": false,\n      "Startup Cost": 0.00,\n      "Total Cost": 0.01,\n      "Plan Rows": 1,\n      "Plan Width": 4,\n      "Disabled": false\n    }\n  }\n]
-- rowcount: 1
-- end-expected
EXPLAIN (FORMAT JSON) SELECT 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_j" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT JSON) SELECT count(*) FROM zz_j;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_j" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT XML) SELECT * FROM zz_j WHERE v = 2;
-- ResultSetMetaData.getColumnTypeName(1)
-- begin-expected
-- columns: QUERY PLAN:json
-- row: [\n  {\n    "Plan": {\n      "Node Type": "Result",\n      "Parallel Aware": false,\n      "Async Capable": false,\n      "Disabled": false\n    }\n  }\n]
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS OFF, FORMAT JSON) SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:xml
-- row: <explain xmlns="http://www.postgresql.org/2009/explain">\n  <Query>\n    <Plan>\n      <Node-Type>Result</Node-Type>\n      <Parallel-Aware>false</Parallel-Aware>\n      <Async-Capable>false</Async-Capable>\n      <Disabled>false</Disabled>\n    </Plan>\n  </Query>\n</explain>
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS OFF, FORMAT XML) SELECT 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rr" does not exist
-- end-expected-error
INSERT INTO zz_rr SELECT g, g FROM generate_series(1,10) g;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rr" does not exist
-- end-expected-error
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM zz_rr WHERE id = 1;
