-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: pg_dump fidelity
-- title: Unrelated singletons
-- begin-expected
-- columns: text:text
-- row: 2024-01-01
-- rowcount: 1
-- end-expected
SELECT '2024-01-01'::date::text;
-- begin-expected
-- columns: text:text
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1::int::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_d7 (p point);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_d7 VALUES ('(1e10,-1e-10)');
-- begin-expected
-- columns: p:text
-- row: (10000000000,-1e-10)
-- rowcount: 1
-- end-expected
SELECT p::text FROM zz_vf2_d7;
-- begin-expected
-- columns: option_name:text | option_value:text
-- row: autovacuum_enabled | false
-- row: fillfactor | 70
-- rowcount: 2
-- end-expected
SELECT option_name, option_value FROM pg_options_to_table(ARRAY['fillfactor=70','autovacuum_enabled=false']) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_t7" does not exist
-- end-expected-error
CREATE STATISTICS zz_vf2_st7 ON a, b FROM zz_vf2_t7;
-- begin-expected
-- columns: pg_get_statisticsobjdef:text
-- rowcount: 0
-- end-expected
SELECT pg_get_statisticsobjdef(oid) FROM pg_statistic_ext WHERE stxname = 'zz_vf2_st7';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_ot8 AS (a integer, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_o8 OF zz_vf2_ot8;
