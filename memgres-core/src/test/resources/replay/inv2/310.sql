-- source: investigation-2026-08.md
-- finding: 310
-- title: ANALYZE records only a timestamp and the table name; every statistic is re-derived from the live table when a catalog is read, with literal nulls and zeros for 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int, v text);
-- begin-expected
-- ok: 300
-- end-expected
INSERT INTO zz_t SELECT g, 'x'||(g%7) FROM generate_series(1,300) g;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE zz_t;
-- begin-expected
-- columns: attname:text | n_distinct:float4
-- row: id | -1
-- row: v | 7
-- rowcount: 2
-- end-expected
SELECT attname::text, n_distinct FROM pg_stats WHERE tablename='zz_t' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (id int) PARTITION BY RANGE (id);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p1 PARTITION OF zz_p FOR VALUES FROM (0) TO (100);
-- begin-expected
-- ok: 60
-- end-expected
INSERT INTO zz_p SELECT g FROM generate_series(1,60) g;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE zz_p;
-- begin-expected
-- columns: relname:text | reltuples:int4
-- row: zz_p | 60
-- row: zz_p1 | 60
-- rowcount: 2
-- end-expected
SELECT relname::text, reltuples::int FROM pg_class WHERE relname IN ('zz_p','zz_p1') ORDER BY 1;
