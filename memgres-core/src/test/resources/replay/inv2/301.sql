-- source: investigation-2026-08.md
-- finding: 301
-- title: The role registry is keyed by name.toLowerCase() and pg_roles.rolname renders that key, so a quoted role name loses the one kind of case PostgreSQL never folds
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE "zz_MiXeD";
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_roles WHERE rolname='zz_MiXeD';
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_roles WHERE rolname='zz_mixed';
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_mixed;
