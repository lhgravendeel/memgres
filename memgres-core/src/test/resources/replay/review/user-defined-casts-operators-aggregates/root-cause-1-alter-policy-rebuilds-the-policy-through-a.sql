-- source: review-2026-08.md
-- finding: Root cause 1: ALTER POLICY rebuilds the policy through a five-argument constructor that hardcodes PERMISSIVE and re-uses the old role list
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 1: ALTER POLICY rebuilds the policy through a five-argument constructor that hardcodes PERMISSIVE and re-uses the old role list
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_rr LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_rt (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_rt VALUES (1),(2),(3);
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_rt TO zz_rr;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_rt ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_perm ON zz_rt FOR SELECT TO zz_rr USING (true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_restr ON zz_rt AS RESTRICTIVE FOR SELECT TO zz_rr USING (id = 1);
-- begin-expected
-- ok: 0
-- end-expected
ALTER POLICY zz_restr ON zz_rt USING (id <= 2);
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_rr;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_rt;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pt (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_pt ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_p1 ON zz_pt FOR SELECT USING (id = 1);
-- begin-expected
-- ok: 0
-- end-expected
ALTER POLICY zz_p1 ON zz_pt TO zz_r1;
-- begin-expected
-- columns: policyname:name | roles:text
-- row: zz_p1 | {zz_r1}
-- rowcount: 1
-- end-expected
SELECT policyname, roles::text FROM pg_policies WHERE tablename='zz_pt';
