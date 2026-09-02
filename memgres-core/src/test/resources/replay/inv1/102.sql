-- source: investigation.md
-- finding: 102
-- title: 127 catalog relations have no attributes ⚠️
-- begin-expected
-- columns: relname:name
-- rowcount: 0
-- end-expected
SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog' AND c.relkind = 'r'
   AND NOT EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0);
--   PG: 0   memgres: 127
--   pg_class, pg_attribute, pg_type, pg_namespace, pg_constraint, pg_description,
--   pg_settings, pg_tables, …;
