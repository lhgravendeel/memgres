-- source: investigation-2026-08.md
-- finding: 362
-- title: No name resolver outside RowContext knows the system columns exist. Every other site goes straight to table.getColumnIndex(name), which only knows user columns:
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
