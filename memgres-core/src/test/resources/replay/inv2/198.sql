-- source: investigation-2026-08.md
-- finding: 198
-- title: The result shape of a statement is discovered by executing the statement. inferResultTypesViaDryRun unparses the body, replaces $N with NULL, appends LIMIT 0 un
-- JDBC with prepareThreshold=1, as a PreparedStatement:
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_w" does not exist
-- end-expected-error
WITH ins AS (INSERT INTO zz_vf2_w VALUES (1) RETURNING a) SELECT a FROM ins;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_w" does not exist
-- end-expected-error
SELECT count(*) FROM zz_vf2_w;
-- and, as a PreparedStatement:
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_sq" does not exist
-- end-expected-error
SELECT nextval('zz_vf2_sq') AS v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf2_s1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pt (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_pf() RETURNS int LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_vf2_pt VALUES (1); RETURN 1; END $$;
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_p1 AS SELECT nextval('zz_vf2_s1') LIMIT 1;
-- begin-expected-error
-- sqlstate: 55000
-- message-like: currval of sequence "zz_vf2_s1" is not yet defined in this session
-- end-expected-error
SELECT currval('zz_vf2_s1');
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_p3 AS SELECT zz_vf2_pf() LIMIT 1;
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_p4 AS SELECT zz_vf2_pf();
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_pt;
-- session A
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_cl" does not exist
-- end-expected-error
DECLARE zz_vf2_cur CURSOR FOR SELECT id FROM zz_vf2_cl ORDER BY id FOR UPDATE;
-- no FETCH at all
-- session B, SET lock_timeout='2000ms'
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_cl" does not exist
-- end-expected-error
UPDATE zz_vf2_cl SET nm='B' WHERE id=3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dc_d (s text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_dc_d VALUES ('1'), ('x');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_dc_s;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_dc_y1 CURSOR FOR SELECT s::int FROM zz_dc_d;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_dc_y2 CURSOR FOR SELECT nextval('zz_dc_s');
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected-error
-- sqlstate: 55000
-- message-like: currval of sequence "zz_dc_s" is not yet defined in this session
-- end-expected-error
SELECT currval('zz_dc_s');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_nv AS SELECT nextval('zz_s2') AS n;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_s2');
