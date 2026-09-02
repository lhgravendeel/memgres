-- source: review-2026-08.md
-- finding: Root cause 5: views, matviews and functions are stored as source text and re-resolved by name
-- area: DDL objects: indexes, sequences, types, functions, triggers, rules, views, partitioning and the catalogs that describe them
-- title: Root cause 5: views, matviews and functions are stored as source text and re-resolved by name
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cbt (a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_cbt VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_cv AS SELECT * FROM zz_vf_cbt;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_cbt RENAME TO zz_vf_cbt2;
-- begin-expected
-- columns: n:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM zz_vf_cv;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_qs;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_qs.dep (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_qs.dep VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_qs.depv AS SELECT a FROM zz_vf_qs.dep;
-- begin-expected
-- ok: 0
-- end-expected
ALTER SCHEMA zz_vf_qs RENAME TO zz_vf_qs3;
-- begin-expected
-- columns: n:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM zz_vf_qs3.depv;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_x1 (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE MATERIALIZED VIEW zz_vf_xm AS SELECT id FROM zz_vf_x1;
-- begin-expected
-- columns: definition:text
-- row:  SELECT id\n   FROM zz_vf_x1;
-- rowcount: 1
-- end-expected
SELECT definition FROM pg_matviews WHERE matviewname='zz_vf_xm';
-- begin-expected
-- ok: 0
-- end-expected
CREATE RECURSIVE VIEW zz_vf_v5r (n) AS SELECT 1 UNION ALL SELECT n+1 FROM zz_vf_v5r WHERE n < 5;
-- begin-expected
-- columns: d:text
-- row:  WITH RECURSIVE zz_vf_v5r(n) AS (\n         SELECT 1 AS "?column?"\n        UNION ALL\n         SELECT (zz_vf_v5r_1.n + 1)\n           FROM zz_vf_v5r zz_vf_v5r_1\n          WHERE (zz_vf_v5r_1.n < 5)\n        )\n SELECT n\n   FROM zz_vf_v5r;
-- rowcount: 1
-- end-expected
SELECT definition AS d FROM pg_views WHERE viewname='zz_vf_v5r';
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_h01(a int, b text DEFAULT 'x') RETURNS int LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE AS $$ SELECT a $$;
-- begin-expected
-- columns: pg_get_function_arguments:text
-- row: a integer, b text DEFAULT 'x'::text
-- rowcount: 1
-- end-expected
SELECT pg_get_function_arguments('zz_vf_h01(int,text)'::regprocedure);
-- begin-expected
-- columns: pg_get_triggerdef:text
-- rowcount: 0
-- end-expected
SELECT pg_get_triggerdef(oid, true) FROM pg_trigger WHERE tgname='zz_vf_tg';
-- begin-expected
-- columns: action_statement:varchar
-- rowcount: 0
-- end-expected
SELECT action_statement FROM information_schema.triggers WHERE trigger_name='zz_vf_tg';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ca" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_t1 BEFORE INSERT ON zz_vf_ca FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ca" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_t2 BEFORE INSERT ON zz_vf_ca FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected
-- columns: trigger_name:name | action_order:int4
-- rowcount: 0
-- end-expected
SELECT trigger_name, action_order FROM information_schema.triggers
 WHERE event_object_table='zz_vf_ca' ORDER BY trigger_name;
