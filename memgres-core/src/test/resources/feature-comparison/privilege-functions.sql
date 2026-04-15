-- ============================================================================
-- Feature Comparison: Privilege Functions & GRANT/REVOKE
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests has_table_privilege, has_schema_privilege, has_database_privilege,
-- and related privilege-checking functions. Also tests GRANT/REVOKE
-- behavior and whether privileges are actually enforced.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS priv_test CASCADE;
CREATE SCHEMA priv_test;
SET search_path = priv_test, public;

CREATE TABLE priv_data (id integer PRIMARY KEY, val text);
INSERT INTO priv_data VALUES (1, 'hello'), (2, 'world');

-- ============================================================================
-- SECTION A: has_table_privilege
-- ============================================================================

-- ============================================================================
-- 1. has_table_privilege: current user on own table
-- ============================================================================

-- begin-expected
-- columns: can_select
-- row: true
-- end-expected
SELECT has_table_privilege('priv_test.priv_data', 'SELECT') AS can_select;

-- ============================================================================
-- 2. has_table_privilege: multiple privilege types
-- ============================================================================

-- begin-expected
-- columns: sel, ins, upd, del
-- row: true, true, true, true
-- end-expected
SELECT
  has_table_privilege('priv_test.priv_data', 'SELECT') AS sel,
  has_table_privilege('priv_test.priv_data', 'INSERT') AS ins,
  has_table_privilege('priv_test.priv_data', 'UPDATE') AS upd,
  has_table_privilege('priv_test.priv_data', 'DELETE') AS del;

-- ============================================================================
-- 3. has_table_privilege with explicit user
-- ============================================================================

-- begin-expected
-- columns: can_select
-- row: true
-- end-expected
SELECT has_table_privilege(current_user, 'priv_test.priv_data', 'SELECT') AS can_select;

-- ============================================================================
-- 4. has_table_privilege with OID
-- ============================================================================

-- begin-expected
-- columns: can_select
-- row: true
-- end-expected
SELECT has_table_privilege('priv_test.priv_data'::regclass, 'SELECT') AS can_select;

-- ============================================================================
-- SECTION B: has_schema_privilege
-- ============================================================================

-- ============================================================================
-- 5. has_schema_privilege: USAGE on own schema
-- ============================================================================

-- begin-expected
-- columns: can_use
-- row: true
-- end-expected
SELECT has_schema_privilege('priv_test', 'USAGE') AS can_use;

-- ============================================================================
-- 6. has_schema_privilege: CREATE on own schema
-- ============================================================================

-- begin-expected
-- columns: can_create
-- row: true
-- end-expected
SELECT has_schema_privilege('priv_test', 'CREATE') AS can_create;

-- ============================================================================
-- 7. has_schema_privilege: public schema
-- ============================================================================

-- begin-expected
-- columns: can_use
-- row: true
-- end-expected
SELECT has_schema_privilege('public', 'USAGE') AS can_use;

-- ============================================================================
-- SECTION C: has_database_privilege
-- ============================================================================

-- ============================================================================
-- 8. has_database_privilege: CONNECT to current db
-- ============================================================================

-- begin-expected
-- columns: can_connect
-- row: true
-- end-expected
SELECT has_database_privilege(current_database(), 'CONNECT') AS can_connect;

-- ============================================================================
-- 9. has_database_privilege: CREATE on current db
-- ============================================================================

-- begin-expected
-- columns: can_create
-- row: true
-- end-expected
SELECT has_database_privilege(current_database(), 'CREATE') AS can_create;

-- ============================================================================
-- SECTION D: has_function_privilege
-- ============================================================================

-- ============================================================================
-- 10. has_function_privilege on user-defined function
-- ============================================================================

CREATE FUNCTION priv_func() RETURNS integer LANGUAGE sql AS $$ SELECT 42 $$;

-- begin-expected
-- columns: can_exec
-- row: true
-- end-expected
SELECT has_function_privilege('priv_test.priv_func()', 'EXECUTE') AS can_exec;

-- ============================================================================
-- SECTION E: Other privilege functions
-- ============================================================================

-- ============================================================================
-- 11. has_sequence_privilege
-- ============================================================================

CREATE SEQUENCE priv_seq;

-- begin-expected
-- columns: can_use
-- row: true
-- end-expected
SELECT has_sequence_privilege('priv_test.priv_seq', 'USAGE') AS can_use;

-- ============================================================================
-- 12. has_column_privilege
-- ============================================================================

-- begin-expected
-- columns: can_select_col
-- row: true
-- end-expected
SELECT has_column_privilege('priv_test.priv_data', 'val', 'SELECT') AS can_select_col;

-- ============================================================================
-- 13. has_type_privilege
-- ============================================================================

-- begin-expected
-- columns: can_use
-- row: true
-- end-expected
SELECT has_type_privilege('integer', 'USAGE') AS can_use;

-- ============================================================================
-- SECTION F: GRANT / REVOKE
-- ============================================================================

-- ============================================================================
-- 14. GRANT SELECT accepted
-- ============================================================================

-- note: GRANT is accepted even if privilege enforcement is limited
-- command: GRANT
GRANT SELECT ON priv_data TO PUBLIC;

-- ============================================================================
-- 15. GRANT multiple privileges
-- ============================================================================

-- command: GRANT
GRANT SELECT, INSERT, UPDATE ON priv_data TO PUBLIC;

-- ============================================================================
-- 16. REVOKE accepted
-- ============================================================================

-- command: REVOKE
REVOKE INSERT ON priv_data FROM PUBLIC;

-- ============================================================================
-- 17. GRANT on schema
-- ============================================================================

-- command: GRANT
GRANT USAGE ON SCHEMA priv_test TO PUBLIC;

-- ============================================================================
-- 18. GRANT ALL on table
-- ============================================================================

-- command: GRANT
GRANT ALL ON priv_data TO PUBLIC;

-- ============================================================================
-- 19. REVOKE ALL on table
-- ============================================================================

-- command: REVOKE
REVOKE ALL ON priv_data FROM PUBLIC;

-- ============================================================================
-- 20. GRANT on sequence
-- ============================================================================

-- command: GRANT
GRANT USAGE, SELECT ON SEQUENCE priv_seq TO PUBLIC;

-- ============================================================================
-- SECTION G: Role management
-- ============================================================================

-- ============================================================================
-- 21. CREATE ROLE
-- ============================================================================

CREATE ROLE priv_testrole;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'priv_testrole') AS exists;

-- ============================================================================
-- 22. GRANT role to role
-- ============================================================================

-- command: GRANT
GRANT priv_testrole TO current_user;

-- ============================================================================
-- 23. DROP ROLE
-- ============================================================================

REVOKE priv_testrole FROM current_user;
DROP ROLE priv_testrole;

-- begin-expected
-- columns: exists
-- row: false
-- end-expected
SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'priv_testrole') AS exists;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA priv_test CASCADE;
SET search_path = public;
