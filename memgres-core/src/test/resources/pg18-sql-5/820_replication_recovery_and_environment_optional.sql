DROP SCHEMA IF EXISTS test_820 CASCADE;
CREATE SCHEMA test_820;
SET search_path TO test_820;

-- begin-expected
-- columns: in_recovery
-- row: f
-- end-expected
SELECT pg_is_in_recovery() AS in_recovery;

-- begin-expected
-- columns: current_wal_lsn_is_not_null
-- row: t
-- end-expected
SELECT pg_current_wal_lsn() IS NOT NULL AS current_wal_lsn_is_not_null;

-- begin-expected
-- columns: database_name_is_not_null
-- row: t
-- end-expected
SELECT current_database() IS NOT NULL AS database_name_is_not_null;

