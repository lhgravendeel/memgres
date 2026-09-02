-- source: review-2026-08.md
-- finding: Root cause 7: each of the role views is built from one source and invents what it cannot find
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 7: each of the role views is built from one source and invents what it cannot find
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lt (a int);
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_locks WHERE locktype='relation' AND relation='zz_lt'::regclass;
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_group WHERE groname IN ('pg_monitor','pg_read_all_data','pg_maintain');
-- begin-expected
-- columns: passwd:text
-- row: ********
-- rowcount: 1
-- end-expected
SELECT passwd FROM pg_user WHERE usename=current_user;
