-- source: investigation.md
-- finding: 69
-- title: `ALTER SCHEMA … RENAME TO` silently does nothing ⚠️ high
-- unrunnable: the report wrote this reproducer abbreviated
CREATE SCHEMA dz_sch_a;
CREATE TABLE dz_sch_a.t (…);
ALTER SCHEMA dz_sch_a RENAME TO dz_sch_b;
-- reports success on both
SELECT count(*) FROM dz_sch_b.t;
--   PG: 2 | mg: 3F000 schema "dz_sch_b" does not exist;;
