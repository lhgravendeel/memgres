-- source: review-2026-08.md
-- finding: Root cause 4: REASSIGN OWNED and DROP OWNED read exactly one role name
-- area: Ownership, default privileges and role membership
-- title: Root cause 4: REASSIGN OWNED and DROP OWNED read exactly one role name
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_qa;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_qb;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q1 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q2 (i int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_q1 OWNER TO zz_qa;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_q2 OWNER TO zz_qb;
-- begin-expected
-- ok: 0
-- end-expected
DROP OWNED BY zz_qa, zz_qb;
-- begin-expected
-- columns: relname:name
-- rowcount: 0
-- end-expected
SELECT relname FROM pg_class WHERE relname IN ('zz_q1','zz_q2') ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole_x" does not exist
-- end-expected-error
DROP OWNED BY zz_a, zz_nosuchrole_x;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_b;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_c;
-- begin-expected
-- ok: 0
-- end-expected
REASSIGN OWNED BY zz_a, zz_b TO zz_c;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_b" already exists
-- end-expected-error
CREATE ROLE zz_b;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CASCADE"
-- end-expected-error
REASSIGN OWNED BY zz_a TO zz_b CASCADE;
