-- source: investigation-2026-08.md
-- finding: 383
-- title: An Execute is a materialize-then-write: the whole result set is computed before a single message reaches the socket. Notices raised during row production are dr
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f7(i int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'row %', i; RETURN i; END $$;
-- begin-expected
-- columns: zz_vf_f7:int4
-- row: 1
-- row: 2
-- row: 3
-- rowcount: 3
-- end-expected
SELECT zz_vf_f7(g) FROM generate_series(1,3) g;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_re (a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_re VALUES (1),(0);
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 10/a FROM zz_vf_re ORDER BY a DESC;
