-- source: investigation-2026-08.md
-- finding: 162
-- title: A row of catalog columns is emitted as hardcoded constants rather than read from the state that already holds the answer — owner, typbasetype, seqtypid/seqcache
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_ro;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ow (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_ow OWNER TO zz_vf_ro;
-- begin-expected
-- columns: tablename:name | tableowner:name
-- row: zz_vf_ow | zz_vf_ro
-- rowcount: 1
-- end-expected
SELECT tablename, tableowner FROM pg_tables WHERE tablename='zz_vf_ow';
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf_d7 AS int DEFAULT 7;
-- begin-expected
-- columns: b:text
-- row: integer
-- rowcount: 1
-- end-expected
SELECT typbasetype::regtype::text AS b FROM pg_type WHERE typname = 'zz_vf_d7';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_sq1 AS integer CACHE 7;
-- begin-expected
-- columns: t:text | seqcache:int8
-- row: integer | 7
-- rowcount: 1
-- end-expected
SELECT seqtypid::regtype::text AS t, seqcache FROM pg_sequence WHERE seqrelid='zz_vf_sq1'::regclass;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_x1 (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE MATERIALIZED VIEW zz_vf_xm AS SELECT id FROM zz_vf_x1;
-- begin-expected
-- columns: relam:oid | relreplident:char
-- row: 2 | d
-- rowcount: 1
-- end-expected
SELECT relam, relreplident FROM pg_class WHERE relname='zz_vf_xm';
-- begin-expected
-- ok: 0
-- end-expected
CREATE UNLOGGED SEQUENCE zz_vf_cq3;
-- begin-expected
-- columns: relpersistence:char
-- row: u
-- rowcount: 1
-- end-expected
SELECT relpersistence FROM pg_class WHERE relname = 'zz_vf_cq3';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_k1 (a int, b int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE STATISTICS zz_vf_ks1 ON a, b FROM zz_vf_k1;
-- begin-expected
-- columns: statistics_name:name | attnames:text | kinds:text
-- rowcount: 0
-- end-expected
SELECT statistics_name, attnames::text, kinds::text FROM pg_stats_ext WHERE statistics_name = 'zz_vf_ks1';
