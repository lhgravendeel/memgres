-- source: investigation-2026-08.md
-- finding: 293
-- title: Role membership is traversed without ever asking whether the member role inherits: hasPrivilegeDirectOrInherited recurses over every membership edge, and the IN
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_b NOINHERIT;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_t TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_a TO zz_b;
-- begin-expected
-- columns: has_table_privilege:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_b','zz_t','SELECT')::text;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_b;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for table zz_t
-- end-expected-error
SELECT count(*) FROM zz_t;
