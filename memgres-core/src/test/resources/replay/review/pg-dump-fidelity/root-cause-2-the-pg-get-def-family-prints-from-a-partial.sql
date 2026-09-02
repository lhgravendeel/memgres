-- source: review-2026-08.md
-- finding: Root cause 2: the pg_get_*def family prints from a partial model, and qualifies by a fixed rule rather than by search_path
-- area: pg_dump fidelity
-- title: Root cause 2: the pg_get_*def family prints from a partial model, and qualifies by a fixed rule rather than by search_path
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_d1 (id integer, d_expr integer DEFAULT (2 + 3) * 4,
                        d_expr2 integer DEFAULT 2 * (3 + 4));
-- begin-expected
-- columns: attname:name | pg_get_expr:text
-- row: d_expr | ((2 + 3) * 4)
-- row: d_expr2 | (2 * (3 + 4))
-- rowcount: 2
-- end-expected
SELECT a.attname, pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d
 JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
 JOIN pg_class c ON c.oid = d.adrelid WHERE c.relname = 'zz_vf2_d1' ORDER BY a.attnum;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_p1" does not exist
-- end-expected-error
CREATE TABLE zz_vf2_c1 (x integer, y integer, z text,
  CONSTRAINT zz_vf2_ckb CHECK (z IS NULL OR length(z) > 2) NO INHERIT,
  CONSTRAINT zz_vf2_fka FOREIGN KEY (x) REFERENCES zz_vf2_p1(a) DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT zz_vf2_fkb FOREIGN KEY (y) REFERENCES zz_vf2_p1(b) MATCH FULL ON DELETE SET NULL DEFERRABLE);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_c1" does not exist
-- end-expected-error
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid = 'zz_vf2_c1'::regclass ORDER BY conname;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_s3.zz_vf2_b3" does not exist
-- end-expected-error
CREATE VIEW zz_vf2_s3.zz_vf2_v3 AS SELECT a, b FROM zz_vf2_s3.zz_vf2_b3 WHERE d > 0;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_s3.zz_vf2_b3" does not exist
-- end-expected-error
CREATE VIEW zz_vf2_s3.zz_vf2_v3b AS SELECT t.a AS aa, count(*) AS n
  FROM zz_vf2_s3.zz_vf2_b3 t GROUP BY t.a HAVING count(*) > 0 ORDER BY t.a;
-- begin-expected
-- columns: set_config:text
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_catalog.set_config('search_path', '', false);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_s3.zz_vf2_v3" does not exist
-- end-expected-error
SELECT pg_get_viewdef('zz_vf2_s3.zz_vf2_v3'::regclass, true);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_s3.zz_vf2_v3b" does not exist
-- end-expected-error
SELECT pg_get_viewdef('zz_vf2_s3.zz_vf2_v3b'::regclass);
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_vf2_s6" does not exist
-- end-expected-error
CREATE CONSTRAINT TRIGGER zz_vf2_tg6c AFTER INSERT ON zz_vf2_s6.zz_vf2_tt6
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zz_vf2_s6.zz_vf2_tf6();
-- begin-expected
-- columns: set_config:text
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_catalog.set_config('search_path', '', false);
-- begin-expected
-- columns: pg_get_triggerdef:text | is_constraint:bool
-- rowcount: 0
-- end-expected
SELECT pg_get_triggerdef(oid, true), tgconstraint <> 0 AS is_constraint
 FROM pg_trigger WHERE tgname = 'zz_vf2_tg6c';
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: no schema has been selected to create in
-- end-expected-error
CREATE FUNCTION zz_vf2_f7() RETURNS integer LANGUAGE sql
  VOLATILE SECURITY DEFINER LEAKPROOF PARALLEL SAFE COST 500
  SET search_path TO public, pg_temp SET work_mem TO '4MB' AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function "zz_vf2_f7()" does not exist
-- end-expected-error
SELECT pg_get_functiondef('zz_vf2_f7()'::regprocedure);
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: no schema has been selected to create in
-- end-expected-error
CREATE TABLE zz_vf2_d1 (id integer, CONSTRAINT zz_vf2_ck1 CHECK (id > 0 AND id < 100));
-- begin-expected
-- columns: pg_get_expr:text
-- rowcount: 0
-- end-expected
SELECT pg_get_expr(conbin, conrelid) FROM pg_constraint WHERE conname = 'zz_vf2_ck1';
