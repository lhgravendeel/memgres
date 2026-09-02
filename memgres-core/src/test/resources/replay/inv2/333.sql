-- source: investigation-2026-08.md
-- finding: 333
-- title: Privileges are a flat PRIV:OBJTYPE:NAME set read literally under the type TABLE, and the policy grammar reads its target with a single readIdentifier().
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf2_s6;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_s6.zz_vf2_t6 (id integer, n integer);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_s6.zz_vf2_t6 ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_vf2_po6 ON zz_vf2_s6.zz_vf2_t6 FOR SELECT USING (n > 0);
