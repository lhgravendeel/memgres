-- source: investigation-2026-08.md
-- finding: 287
-- title: pg_size_pretty and pg_size_bytes reimplement PostgreSQL's size_pretty_units table and pg_size_bytes' scanner by hand, with the wrong thresholds, truncation inst
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
