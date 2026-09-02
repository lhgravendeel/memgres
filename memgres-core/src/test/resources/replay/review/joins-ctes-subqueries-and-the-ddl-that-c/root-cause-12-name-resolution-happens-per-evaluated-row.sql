-- source: review-2026-08.md
-- finding: Root cause 12: name resolution happens per evaluated row, not in an analysis pass
-- area: Joins, CTEs, subqueries — and the DDL that came with them
-- title: Root cause 12: name resolution happens per evaluated row, not in an analysis pass
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_e0 (a int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zz_e0"
-- end-expected-error
SELECT count(*) FROM zz_e0 x WHERE public.zz_e0.a = 1;
-- empty table
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zz_nosuchtable"
-- end-expected-error
SELECT count(*) FROM zz_e0 x WHERE zz_nosuchtable.a = 1;
-- empty table
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_e0 VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zz_e0"
-- end-expected-error
SELECT count(*) FROM zz_e0 x WHERE public.zz_e0.a = 1;
-- one row
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zz_e0"
-- end-expected-error
SELECT count(*) FROM (SELECT a FROM zz_e0) s WHERE zz_e0.a = 1;
