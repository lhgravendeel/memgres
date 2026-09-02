-- source: investigation-2026-08.md
-- finding: 91
-- title: Row-level guards (table privilege, RLS UPDATE policy, view WITH CHECK OPTION) are attached to the simple UPDATE path only, so the ON CONFLICT DO UPDATE and UPDA
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s (id int primary key, n int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_s VALUES (1,10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT, INSERT ON zz_s TO zz_r;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_r;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for table zz_s
-- end-expected-error
INSERT INTO zz_s VALUES (1,1) ON CONFLICT (id) DO UPDATE SET n = 5;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_rls (id int primary key, owner text, n int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_rls VALUES (1,'zz_r',10),(2,'other',20);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_r" already exists
-- end-expected-error
CREATE ROLE zz_r LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT, INSERT, UPDATE ON zz_rls TO zz_r;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_rls ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY p1 ON zz_rls FOR SELECT USING (true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY p2 ON zz_rls FOR INSERT WITH CHECK (true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY p3 ON zz_rls FOR UPDATE USING (owner = current_user) WITH CHECK (n < 100);
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_r;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy (USING expression) for table "zz_rls"
-- end-expected-error
INSERT INTO zz_rls VALUES (2,'other',7) ON CONFLICT (id) DO UPDATE SET n = 7;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_base (id int, n int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_base VALUES (1, 5);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vw AS SELECT id, n FROM zz_base WHERE n < 10 WITH CHECK OPTION;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_src (k int, nv int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_src VALUES (1, 500);
-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zz_vw"
-- end-expected-error
UPDATE zz_vw v SET n = s.nv FROM zz_src s WHERE v.id = s.k;
-- same table, roles and policies as the previous entry
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_r;
-- begin-expected
-- columns: id:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT id FROM zz_rls ORDER BY id FOR UPDATE;
