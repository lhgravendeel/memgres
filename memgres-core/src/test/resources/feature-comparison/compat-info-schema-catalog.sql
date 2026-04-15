-- ============================================================================
-- Feature Comparison: information_schema Catalog Name
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18: information_schema views report the actual database name in
-- table_catalog, catalog_name, constraint_catalog, etc.
--
-- Memgres: Always reports "memgres" as the catalog name, regardless of the
-- actual database name used in the connection.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- note: ...      -> informational comment
-- ============================================================================

DROP TABLE IF EXISTS is_cat_test CASCADE;
CREATE TABLE is_cat_test (id int PRIMARY KEY, name text NOT NULL);

-- ============================================================================
-- 1. table_catalog should match current_database()
-- ============================================================================

-- note: PG reports the actual database name. Memgres reports "memgres".

-- begin-expected
-- columns: matches
-- row: true
-- end-expected
SELECT table_catalog = current_database() AS matches
FROM information_schema.tables
WHERE table_name = 'is_cat_test' AND table_schema = 'public';

-- ============================================================================
-- 2. columns.table_catalog should match current_database()
-- ============================================================================

-- begin-expected
-- columns: matches
-- row: true
-- end-expected
SELECT table_catalog = current_database() AS matches
FROM information_schema.columns
WHERE table_name = 'is_cat_test' AND column_name = 'id';

-- ============================================================================
-- 3. schemata.catalog_name should match current_database()
-- ============================================================================

-- begin-expected
-- columns: matches
-- row: true
-- end-expected
SELECT catalog_name = current_database() AS matches
FROM information_schema.schemata
WHERE schema_name = 'public';

-- ============================================================================
-- 4. table_constraints.constraint_catalog should match
-- ============================================================================

-- begin-expected
-- columns: matches
-- row: true
-- end-expected
SELECT constraint_catalog = current_database() AS matches
FROM information_schema.table_constraints
WHERE table_name = 'is_cat_test'
LIMIT 1;

-- ============================================================================
-- 5. routines.specific_catalog should match
-- ============================================================================

DROP FUNCTION IF EXISTS is_cat_func();
CREATE FUNCTION is_cat_func() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected
-- columns: matches
-- row: true
-- end-expected
SELECT specific_catalog = current_database() AS matches
FROM information_schema.routines
WHERE routine_name = 'is_cat_func';

DROP FUNCTION is_cat_func();

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE is_cat_test;
