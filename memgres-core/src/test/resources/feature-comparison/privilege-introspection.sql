-- ============================================================================
-- Feature Comparison: privilege introspection truthfulness
-- Target: PostgreSQL 18 vs Memgres
-- Covers the has_*_privilege family and pg_has_role: every one of them resolves
-- its role, its object and its privilege name before answering, so a name that
-- does not exist is an error rather than a permissive "true". Also covers the
-- GRANT validations that reject a column privilege no column can carry and a
-- grant option handed to PUBLIC.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP VIEW IF EXISTS pit_vw CASCADE;
DROP TABLE IF EXISTS pit_pv CASCADE;
DROP SEQUENCE IF EXISTS pit_seq CASCADE;
DROP FUNCTION IF EXISTS pit_f(int) CASCADE;
DROP SCHEMA IF EXISTS pit_s CASCADE;
DROP ROLE IF EXISTS pit_r;

CREATE TABLE pit_pv (i int, j text);
CREATE VIEW pit_vw AS SELECT i FROM pit_pv;
CREATE SEQUENCE pit_seq;
CREATE SCHEMA pit_s;
CREATE ROLE pit_r;
CREATE FUNCTION pit_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1';

-- ============================================================================
-- SECTION A: has_table_privilege — privilege names
-- ============================================================================

SELECT has_table_privilege('pit_pv', 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_pv', 'select')::text AS ok;
SELECT has_table_privilege('pit_pv', ' SELECT ')::text AS ok;
SELECT has_table_privilege('pit_pv', 'SELECT WITH GRANT OPTION')::text AS ok;
SELECT has_table_privilege('pit_pv', 'INSERT, UPDATE')::text AS ok;
SELECT has_table_privilege('pit_pv', 'MAINTAIN')::text AS ok;
SELECT has_table_privilege('pit_pv', 'TRIGGER')::text AS ok;
SELECT has_table_privilege('pit_pv', 'TRUNCATE')::text AS ok;
SELECT has_table_privilege('pit_pv', 'REFERENCES')::text AS ok;

-- A privilege name no table carries is 22023, not "true".
SELECT has_table_privilege('pit_pv', 'NOSUCHPRIV');
SELECT has_table_privilege('pit_pv', 'USAGE');
SELECT has_table_privilege('pit_pv', 'EXECUTE');
SELECT has_table_privilege('pit_pv', 'CONNECT');
SELECT has_table_privilege('pit_pv', 'ALL');
SELECT has_table_privilege('pit_pv', '');
SELECT has_table_privilege('pit_pv', 'SELECT,');
SELECT has_table_privilege('pit_pv', 'SELECT WITH  GRANT OPTION');

-- ============================================================================
-- SECTION B: has_table_privilege — objects and roles
-- ============================================================================

SELECT has_table_privilege('public.pit_pv', 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_pv'::regclass, 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_vw', 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_seq', 'SELECT')::text AS ok;
SELECT has_table_privilege('pg_class', 'SELECT')::text AS ok;
SELECT has_table_privilege(current_user, 'pg_class', 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_r', 'pit_pv', 'SELECT')::text AS ok;
SELECT has_table_privilege('public', 'pit_pv', 'SELECT')::text AS ok;

-- Strict: a NULL argument makes the answer unknown, not false.
SELECT has_table_privilege(NULL, 'pit_pv', 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_pv', NULL)::text AS ok;
SELECT has_table_privilege(NULL, 'SELECT')::text AS ok;

-- A relation that does not exist is 42P01; a role that does not exist is 42704,
-- and the role is resolved first.
SELECT has_table_privilege('pit_no_such_table', 'SELECT');
SELECT has_table_privilege('public.pit_no_such_table', 'SELECT');
SELECT has_table_privilege('pit_no_such_table', 'NOSUCHPRIV');
SELECT has_table_privilege('pit_no_such_role', 'pit_pv', 'SELECT');
SELECT has_table_privilege('pit_no_such_role', 'pit_no_such_table', 'NOSUCHPRIV');

-- ============================================================================
-- SECTION C: has_column_privilege
-- ============================================================================

SELECT has_column_privilege('pit_pv', 'i', 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_pv', 'i', 'INSERT')::text AS ok;
SELECT has_column_privilege('pit_pv', 'i', 'UPDATE')::text AS ok;
SELECT has_column_privilege('pit_pv', 'i', 'REFERENCES')::text AS ok;
SELECT has_column_privilege('pit_pv', 'i', 'SELECT WITH GRANT OPTION')::text AS ok;
SELECT has_column_privilege('pit_r', 'pit_pv', 'i', 'SELECT')::text AS ok;

-- By attnum: out of range is unknown, not an error.
SELECT has_column_privilege('pit_pv', 1::int2, 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_pv', 2::int2, 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_pv', 0::int2, 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_pv', 99::int2, 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_pv', (-1)::int2, 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_pv', NULL, 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_pv', 'i', NULL)::text AS ok;

-- By name: a column that is not there is 42703, and DELETE is not a column privilege.
SELECT has_column_privilege('pit_pv', 'nosuchcol', 'SELECT');
SELECT has_column_privilege('public.pit_pv', 'nosuchcol', 'SELECT');
SELECT has_column_privilege('pit_pv', 'nosuchcol', 'NOSUCHPRIV');
SELECT has_column_privilege('pit_pv', 'i', 'NOSUCHPRIV');
SELECT has_column_privilege('pit_pv', 'i', 'USAGE');
SELECT has_column_privilege('pit_pv', 'i', 'DELETE');
SELECT has_column_privilege('pit_no_such_table', 'i', 'SELECT');
SELECT has_column_privilege('pit_no_such_table', 'nosuchcol', 'NOSUCHPRIV');
SELECT has_column_privilege('pit_no_such_role', 'pit_pv', 'i', 'SELECT');

-- ============================================================================
-- SECTION D: has_any_column_privilege
-- ============================================================================

SELECT has_any_column_privilege('pit_pv', 'SELECT')::text AS ok;
SELECT has_any_column_privilege('pit_pv', 'INSERT')::text AS ok;
SELECT has_any_column_privilege('pit_pv', 'UPDATE')::text AS ok;
SELECT has_any_column_privilege('pit_pv', 'REFERENCES')::text AS ok;
SELECT has_any_column_privilege('pit_r', 'pit_pv', 'SELECT')::text AS ok;
SELECT has_any_column_privilege(NULL, 'SELECT')::text AS ok;
SELECT has_any_column_privilege('pit_pv', 'NOSUCHPRIV');
SELECT has_any_column_privilege('pit_pv', 'DELETE');
SELECT has_any_column_privilege('pit_no_such_table', 'SELECT');
SELECT has_any_column_privilege('pit_no_such_role', 'pit_pv', 'SELECT');

-- ============================================================================
-- SECTION E: has_schema_privilege
-- ============================================================================

SELECT has_schema_privilege('pit_s', 'USAGE')::text AS ok;
SELECT has_schema_privilege('pit_s', 'CREATE')::text AS ok;
SELECT has_schema_privilege('public', 'USAGE')::text AS ok;
SELECT has_schema_privilege('pit_r', 'pit_s', 'USAGE')::text AS ok;
SELECT has_schema_privilege('pit_r', 'public', 'USAGE')::text AS ok;
-- PG 15 dropped PUBLIC's CREATE on the public schema.
SELECT has_schema_privilege('pit_r', 'public', 'CREATE')::text AS ok;
SELECT has_schema_privilege('pit_r', 'pg_catalog', 'USAGE')::text AS ok;
SELECT has_schema_privilege('pit_r', 'pg_catalog', 'CREATE')::text AS ok;
SELECT has_schema_privilege(NULL, 'USAGE')::text AS ok;
SELECT has_schema_privilege('pit_s', NULL)::text AS ok;

SELECT has_schema_privilege('pit_no_such_schema', 'USAGE');
SELECT has_schema_privilege('pit_s', 'NOSUCHPRIV');
SELECT has_schema_privilege('pit_s', 'SELECT');
SELECT has_schema_privilege('pit_no_such_role', 'pit_s', 'USAGE');

-- ============================================================================
-- SECTION F: has_database_privilege
-- ============================================================================

SELECT has_database_privilege(current_database(), 'CONNECT')::text AS ok;
SELECT has_database_privilege(current_database(), 'CREATE')::text AS ok;
SELECT has_database_privilege(current_database(), 'TEMP')::text AS ok;
SELECT has_database_privilege(current_database(), 'TEMPORARY')::text AS ok;
-- PUBLIC holds CONNECT and TEMPORARY by default; CREATE is the owner's.
SELECT has_database_privilege('pit_r', current_database(), 'CONNECT')::text AS ok;
SELECT has_database_privilege('pit_r', current_database(), 'TEMP')::text AS ok;
SELECT has_database_privilege('pit_r', current_database(), 'CREATE')::text AS ok;
SELECT has_database_privilege('pit_r', current_database(), 'CONNECT WITH GRANT OPTION')::text AS ok;
SELECT has_database_privilege(NULL, 'CONNECT')::text AS ok;

SELECT has_database_privilege(current_database(), 'NOSUCHPRIV');
SELECT has_database_privilege('pit_no_such_db', 'CONNECT');
SELECT has_database_privilege('pit_no_such_role', current_database(), 'CONNECT');

-- ============================================================================
-- SECTION G: has_sequence_privilege
-- ============================================================================

SELECT has_sequence_privilege('pit_seq', 'USAGE')::text AS ok;
SELECT has_sequence_privilege('pit_seq', 'SELECT')::text AS ok;
SELECT has_sequence_privilege('pit_seq', 'UPDATE')::text AS ok;
SELECT has_sequence_privilege('pit_r', 'pit_seq', 'USAGE')::text AS ok;
SELECT has_sequence_privilege(NULL, 'USAGE')::text AS ok;

SELECT has_sequence_privilege('pit_seq', 'INSERT');
SELECT has_sequence_privilege('pit_seq', 'NOSUCHPRIV');
SELECT has_sequence_privilege('pit_no_such_seq', 'USAGE');
-- A table is a relation but not a sequence.
SELECT has_sequence_privilege('pit_pv', 'USAGE');
SELECT has_sequence_privilege('pit_no_such_role', 'pit_seq', 'USAGE');

-- ============================================================================
-- SECTION H: has_function_privilege and the other object kinds
-- ============================================================================

SELECT has_function_privilege('pit_f(int)', 'EXECUTE')::text AS ok;
SELECT has_function_privilege('now()', 'EXECUTE')::text AS ok;
SELECT has_function_privilege(NULL, 'EXECUTE')::text AS ok;
SELECT has_function_privilege('pit_f(int)', 'NOSUCHPRIV');
SELECT has_function_privilege('pit_f(int)', 'SELECT');
SELECT has_function_privilege('pit_no_such_fn(int)', 'EXECUTE');
SELECT has_function_privilege('pit_no_such_role', 'pit_f(int)', 'EXECUTE');

SELECT has_language_privilege('sql', 'USAGE')::text AS ok;
SELECT has_language_privilege('pit_r', 'plpgsql', 'USAGE')::text AS ok;
SELECT has_language_privilege('nosuchlang', 'USAGE');
SELECT has_language_privilege('sql', 'NOSUCHPRIV');

SELECT has_type_privilege('integer', 'USAGE')::text AS ok;
SELECT has_type_privilege('int4', 'USAGE')::text AS ok;
SELECT has_type_privilege('pit_r', 'text', 'USAGE')::text AS ok;
SELECT has_type_privilege('pit_nosuchtype', 'USAGE');
SELECT has_type_privilege('int4', 'NOSUCHPRIV');

SELECT has_tablespace_privilege('pg_default', 'CREATE')::text AS ok;
SELECT has_tablespace_privilege('pit_nosuchts', 'CREATE');
SELECT has_tablespace_privilege('pg_default', 'NOSUCHPRIV');

SELECT has_parameter_privilege('work_mem', 'SET')::text AS ok;
SELECT has_parameter_privilege('work_mem', 'ALTER SYSTEM')::text AS ok;
SELECT has_parameter_privilege('pit_nosuchparam', 'SET')::text AS ok;
SELECT has_parameter_privilege('work_mem', 'NOSUCHPRIV');

SELECT has_server_privilege('pit_nosuchsrv', 'USAGE');
SELECT has_foreign_data_wrapper_privilege('pit_nosuchfdw', 'USAGE');

-- ============================================================================
-- SECTION I: pg_has_role
-- ============================================================================

SELECT pg_has_role('pit_r', 'USAGE')::text AS ok;
SELECT pg_has_role('pit_r', 'SET')::text AS ok;
SELECT pg_has_role('pit_r', 'usage')::text AS ok;
SELECT pg_has_role('memgres', 'pit_r', 'USAGE')::text AS ok;
SELECT pg_has_role('pit_r', 'pit_r', 'USAGE')::text AS ok;
SELECT pg_has_role('pit_r', 'memgres', 'USAGE')::text AS ok;
SELECT pg_has_role(NULL, 'USAGE')::text AS ok;

SELECT pg_has_role('pit_r', 'NOSUCHPRIV');
-- pg_has_role does not accept PUBLIC, unlike the has_*_privilege family.
SELECT pg_has_role('public', 'USAGE');
SELECT pg_has_role('pit_no_such_role', 'USAGE');
SELECT pg_has_role('pit_no_such_role', 'pit_r', 'USAGE');
SELECT pg_has_role('memgres', 'pit_no_such_role', 'USAGE');

-- ============================================================================
-- SECTION J: GRANT validation
-- ============================================================================

-- Only SELECT/INSERT/UPDATE/REFERENCES can be granted on a column.
GRANT DELETE (i) ON pit_pv TO PUBLIC;
GRANT TRUNCATE (i) ON pit_pv TO PUBLIC;
GRANT TRIGGER (i) ON pit_pv TO PUBLIC;
GRANT MAINTAIN (i) ON pit_pv TO PUBLIC;
GRANT USAGE (i) ON pit_pv TO PUBLIC;

-- A name that is no privilege at all is a syntax error, reported lower-cased.
GRANT NOSUCHPRIV (i) ON pit_pv TO PUBLIC;
GRANT nosuchpriv ON pit_pv TO PUBLIC;

-- On a relation, USAGE is rejected as a table privilege and the rest as relation ones.
GRANT USAGE ON pit_pv TO PUBLIC;
GRANT EXECUTE ON pit_pv TO PUBLIC;
GRANT EXECUTE ON TABLE pit_pv TO PUBLIC;
GRANT CONNECT ON pit_pv TO PUBLIC;
GRANT CREATE ON pit_pv TO PUBLIC;

-- A grant option is a property of a role, and PUBLIC is not one.
GRANT SELECT ON pit_pv TO PUBLIC WITH GRANT OPTION;
GRANT SELECT ON pit_pv TO pit_r, PUBLIC WITH GRANT OPTION;

-- The object is resolved before either check.
GRANT SELECT ON pit_nosuch_tbl TO PUBLIC;
GRANT SELECT ON pit_nosuch TO PUBLIC WITH GRANT OPTION;
GRANT DELETE (i) ON pit_nosuch TO PUBLIC;
GRANT SELECT ON pit_pv TO pit_nosuch_role;
GRANT SELECT (nosuchcol) ON pit_pv TO PUBLIC;

-- ALTER DEFAULT PRIVILEGES resolves its schema and role too.
ALTER DEFAULT PRIVILEGES IN SCHEMA pit_nosuch GRANT SELECT ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pit_nosuch REVOKE SELECT ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE pit_nosuchrole IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC;

-- ============================================================================
-- SECTION K: what a successful GRANT then reports
-- ============================================================================

GRANT SELECT (i) ON pit_pv TO PUBLIC;
SELECT has_column_privilege('pit_r', 'pit_pv', 'i', 'SELECT')::text AS ok;
SELECT has_column_privilege('pit_r', 'pit_pv', 'j', 'SELECT')::text AS ok;
SELECT has_any_column_privilege('pit_r', 'pit_pv', 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_r', 'pit_pv', 'SELECT')::text AS ok;

GRANT INSERT (i), UPDATE (i), REFERENCES (i) ON pit_pv TO PUBLIC;
GRANT SELECT ON pit_pv TO pit_r;
SELECT has_table_privilege('pit_r', 'pit_pv', 'SELECT')::text AS ok;
SELECT has_table_privilege('pit_r', 'pit_pv', 'DELETE')::text AS ok;

GRANT ALL ON pit_pv TO PUBLIC;
SELECT has_table_privilege('pit_r', 'pit_pv', 'DELETE')::text AS ok;
SELECT has_table_privilege('pit_r', 'pit_pv', 'SELECT, INSERT')::text AS ok;
SELECT has_table_privilege('pit_r', 'pit_pv', 'NOSUCHPRIV, SELECT');

GRANT USAGE ON SCHEMA pit_s TO pit_r;
SELECT has_schema_privilege('pit_r', 'pit_s', 'USAGE')::text AS ok;
SELECT has_schema_privilege('pit_r', 'pit_s', 'CREATE')::text AS ok;

GRANT USAGE ON SEQUENCE pit_seq TO pit_r;
SELECT has_sequence_privilege('pit_r', 'pit_seq', 'USAGE')::text AS ok;
SELECT has_sequence_privilege('pit_r', 'pit_seq', 'SELECT')::text AS ok;

-- ============================================================================
-- Teardown
-- ============================================================================

DROP VIEW IF EXISTS pit_vw CASCADE;
DROP TABLE IF EXISTS pit_pv CASCADE;
DROP SEQUENCE IF EXISTS pit_seq CASCADE;
DROP FUNCTION IF EXISTS pit_f(int) CASCADE;
DROP SCHEMA IF EXISTS pit_s CASCADE;
DROP ROLE IF EXISTS pit_r;
