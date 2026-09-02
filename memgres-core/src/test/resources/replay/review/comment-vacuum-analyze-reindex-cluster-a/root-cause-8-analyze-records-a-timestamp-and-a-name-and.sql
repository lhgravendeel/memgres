-- source: review-2026-08.md
-- finding: Root cause 8: ANALYZE records a timestamp and a name, and every statistic is re-derived from the live table afterwards
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 8: ANALYZE records a timestamp and a name, and every statistic is re-derived from the live table afterwards
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
-- columns: attname:text | mcv:bool | hb:bool | correlation:float4
-- row: id | f | t | 1
-- row: v | t | f | 0.15719064
-- rowcount: 2
-- end-expected
SELECT attname::text, most_common_vals IS NOT NULL AS mcv, histogram_bounds IS NOT NULL AS hb,
       correlation FROM pg_stats WHERE tablename='zz_t' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 500
-- end-expected
INSERT INTO zz_t SELECT CASE WHEN g%10=0 THEN NULL ELSE g%5 END FROM generate_series(1,500) g;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE zz_t;
-- begin-expected
-- columns: snf:bool
-- row: t
-- row: t
-- rowcount: 2
-- end-expected
SELECT stanullfrac > 0 AS snf FROM pg_statistic WHERE starelid='zz_t'::regclass;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_t VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
VACUUM zz_t;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE zz_t;
-- begin-expected
-- columns: vacuum_count:int8 | analyze_count:int8
-- row: 1 | 3
-- rowcount: 1
-- end-expected
SELECT vacuum_count, analyze_count FROM pg_stat_user_tables WHERE relname='zz_t';
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
