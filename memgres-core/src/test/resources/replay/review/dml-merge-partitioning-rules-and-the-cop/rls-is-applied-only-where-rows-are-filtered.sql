-- source: review-2026-08.md
-- finding: RLS is applied only where rows are filtered
-- area: DML, MERGE, partitioning, rules and the COPY/extended-protocol surface
-- title: RLS is applied only where rows are filtered
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_s2 (id int primary key, owner text, n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_role LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT, INSERT ON zz_vf_s2 TO zz_vf_role;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_s2 ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_vf_ins ON zz_vf_s2 FOR INSERT WITH CHECK (true);
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_role;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zz_vf_s2"
-- end-expected-error
INSERT INTO zz_vf_s2 VALUES (4,'zz_vf_role',40) RETURNING id;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pe (id int, s text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_pe VALUES (1,'x'),(2,'0');
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_role2 LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT, INSERT, UPDATE ON zz_vf_pe TO zz_vf_role2;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_pe ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_vf_pep ON zz_vf_pe FOR ALL USING (10 / s::int > 0) WITH CHECK (10 / s::int > 0);
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_role2;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT id FROM zz_vf_pe ORDER BY id;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE zz_vf_pe SET s = 'y' WHERE id = 2;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO zz_vf_pe VALUES (3, '0');
