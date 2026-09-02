-- source: review-2026-08.md
-- finding: Root cause 10: pg_size_pretty and pg_size_bytes reimplement PostgreSQL's unit tables and get them wrong
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 10: pg_size_pretty and pg_size_bytes reimplement PostgreSQL's unit tables and get them wrong
-- begin-expected
-- columns: pg_size_pretty:text
-- row: 1024 bytes
-- rowcount: 1
-- end-expected
SELECT pg_size_pretty(1024::bigint);
-- begin-expected
-- columns: pg_size_pretty:text
-- row: 10239 bytes
-- rowcount: 1
-- end-expected
SELECT pg_size_pretty(10239::bigint);
-- begin-expected
-- columns: pg_size_pretty:text
-- row: 1536 kB
-- rowcount: 1
-- end-expected
SELECT pg_size_pretty((1536*1024)::bigint);
-- begin-expected
-- columns: pg_size_pretty:text
-- row: 20 MB
-- rowcount: 1
-- end-expected
SELECT pg_size_pretty((20479*1024)::bigint);
-- begin-expected
-- columns: pg_size_pretty:text
-- row: 5120 TB
-- rowcount: 1
-- end-expected
SELECT pg_size_pretty(1024::bigint*1024*1024*1024*1024*5);
-- begin-expected
-- columns: pg_size_pretty:text
-- row: 3072 PB
-- rowcount: 1
-- end-expected
SELECT pg_size_pretty(1024::bigint*1024*1024*1024*1024*1024*3);
-- begin-expected
-- columns: pg_size_bytes:int8
-- row: -1024
-- rowcount: 1
-- end-expected
SELECT pg_size_bytes('-1 kB');
-- begin-expected
-- columns: pg_size_bytes:int8
-- row: 2048
-- rowcount: 1
-- end-expected
SELECT pg_size_bytes('+2 kB');
-- begin-expected
-- columns: pg_size_bytes:int8
-- row: 1000
-- rowcount: 1
-- end-expected
SELECT pg_size_bytes('1e3');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid size: "1 byte"
-- end-expected-error
SELECT pg_size_bytes('1 byte');
-- begin-expected
-- columns: pg_size_bytes:int8
-- row: 1741
-- rowcount: 1
-- end-expected
SELECT pg_size_bytes('1.7 kB');
-- begin-expected
-- columns: pg_size_bytes:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT pg_size_bytes('0.9');
