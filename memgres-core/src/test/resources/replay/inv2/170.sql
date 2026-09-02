-- source: investigation-2026-08.md
-- finding: 170
-- title: each of the role and lock catalog views is built from a single map and fills the columns it has no source for with a literal — a cross product for pg_locks, get
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
