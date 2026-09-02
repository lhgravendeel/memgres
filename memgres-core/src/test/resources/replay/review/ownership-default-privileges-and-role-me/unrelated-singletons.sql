-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Ownership, default privileges and role membership
-- title: Unrelated singletons
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
