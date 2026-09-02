-- source: review-2026-08.md
-- finding: Root cause 7: RLS bypass is decided from superuser/owner alone, both gated on FORCE
-- area: System catalogs, information_schema and security
-- title: Root cause 7: RLS bypass is decided from superuser/owner alone, both gated on FORCE
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ow (id int);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_ow VALUES (1),(2),(3),(4);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_ow ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_ow FORCE ROW LEVEL SECURITY;
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_ow;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_b LOGIN BYPASSRLS;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_bt (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_bt VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_bt ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_bp ON zz_bt FOR SELECT USING (id = 1);
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_bt TO zz_b;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_b;
-- begin-expected
-- columns: id:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT id FROM zz_bt ORDER BY id;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s1 (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_s1 VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_sr LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT ALL ON zz_s1 TO zz_sr;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_s1 ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_sp ON zz_s1 FOR SELECT TO zz_sr USING (id = 1);
-- begin-expected
-- ok: 0
-- end-expected
SET row_security = off;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_sr;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: query would be affected by row-level security policy for table "zz_s1"
-- end-expected-error
SELECT count(*) FROM zz_s1;
