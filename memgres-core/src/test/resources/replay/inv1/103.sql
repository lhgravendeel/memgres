-- source: investigation.md
-- finding: 103
-- title: `pg_cast` contains 76 dangling type references ⚠️
-- begin-expected
-- columns: oid:oid
-- rowcount: 0
-- end-expected
SELECT c.oid FROM pg_cast c
 WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.castsource)
    OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.casttarget);
--   PG: 0   memgres: 76;
