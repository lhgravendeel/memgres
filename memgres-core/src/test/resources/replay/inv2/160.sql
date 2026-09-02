-- source: investigation-2026-08.md
-- finding: 160
-- title: Views, matviews and functions are kept as source text and re-resolved by name at use time rather than as a bound parse tree. A rename of the base table or the s
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
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_o (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_tg BEFORE INSERT ON zz_vf_o FOR EACH ROW EXECUTE FUNCTION zz_vf_f();
-- begin-expected
-- columns: pg_get_triggerdef:text
-- row: CREATE TRIGGER zz_vf_tg BEFORE INSERT ON zz_vf_o FOR EACH ROW EXECUTE FUNCTION zz_vf_f()
-- rowcount: 1
-- end-expected
SELECT pg_get_triggerdef(oid, true) FROM pg_trigger WHERE tgname='zz_vf_tg';
-- begin-expected
-- columns: action_statement:varchar
-- row: EXECUTE FUNCTION zz_vf_f()
-- rowcount: 1
-- end-expected
SELECT action_statement FROM information_schema.triggers WHERE trigger_name='zz_vf_tg';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ca (id int primary key, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_trgfn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_t1 BEFORE INSERT ON zz_vf_ca FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_t2 BEFORE INSERT ON zz_vf_ca FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected
-- columns: trigger_name:name | action_order:int4
-- row: zz_vf_t1 | 1
-- row: zz_vf_t2 | 2
-- rowcount: 2
-- end-expected
SELECT trigger_name, action_order FROM information_schema.triggers WHERE event_object_table='zz_vf_ca' ORDER BY trigger_name;
-- and, with a WHEN clause / a REFERENCING clause:
-- begin-expected
-- columns: action_condition:varchar | action_reference_old_table:name | action_reference_new_table:name
-- rowcount: 0
-- end-expected
SELECT action_condition, action_reference_old_table, action_reference_new_table FROM information_schema.triggers WHERE trigger_name='zz_vf_tg12';
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
