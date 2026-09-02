-- source: review-2026-08.md
-- finding: Root cause 8: no name resolver outside RowContext knows the system columns exist
-- area: System columns, row locking and TABLESAMPLE
-- title: Root cause 8: no name resolver outside RowContext knows the system columns exist
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pol (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_vf2_pol_p ON zz_vf2_pol USING (ctid IS NOT NULL);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_gb (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_gb VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "zz_vf2_gb.ctid" must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT id FROM zz_vf2_gb GROUP BY id ORDER BY ctid;
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
