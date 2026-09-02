-- source: investigation-2026-08.md
-- finding: 364
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pt (id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_pt VALUES (1);
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type tid to bigint
-- end-expected-error
SELECT ctid::bigint FROM zz_vf2_pt;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_tr (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_tr VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 2202G
-- message-like: TABLESAMPLE REPEATABLE parameter cannot be null
-- end-expected-error
SELECT count(*) FROM zz_vf2_tr TABLESAMPLE BERNOULLI (100) REPEATABLE (NULL);
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_tr TABLESAMPLE BERNOULLI (100) REPEATABLE (0.0);
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_tr TABLESAMPLE BERNOULLI (100) REPEATABLE (1e300);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of REPEATABLE must be type double precision, not type boolean
-- end-expected-error
SELECT count(*) FROM zz_vf2_tr TABLESAMPLE BERNOULLI (100) REPEATABLE (true);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_tm" does not exist
-- end-expected-error
SELECT count(*) FROM zz_vf2_tm TABLESAMPLE "bernoulli" (100);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_tm" does not exist
-- end-expected-error
SELECT count(*) FROM zz_vf2_tm TABLESAMPLE "SYSTEM" (100);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: index creation on system columns is not supported
-- end-expected-error
CREATE TABLE zz_vf2_u2 (id int, PRIMARY KEY (ctid));
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: cannot use system column "xmin" in column generation expression
-- end-expected-error
CREATE TABLE zz_vf2_u3 (id int, c int GENERATED ALWAYS AS (xmin::int) STORED);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_u4" does not exist
-- end-expected-error
ALTER TABLE zz_vf2_u4 DROP COLUMN ctid;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_u4" does not exist
-- end-expected-error
INSERT INTO zz_vf2_u4 VALUES (1) ON CONFLICT (ctid) DO NOTHING;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_srf (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_srf VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with set-returning functions in the target list
-- end-expected-error
SELECT generate_series(1,2) FROM zz_vf2_srf FOR UPDATE;
