-- ============================================================================
-- Feature Comparison: Round 14 — Auth & privileges
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_auth CASCADE;
CREATE SCHEMA r14_auth;
SET search_path = r14_auth, public;

-- ============================================================================
-- SECTION A: GRANT MAINTAIN (PG 17+)
-- ============================================================================

CREATE ROLE r14_auth_m1;
CREATE TABLE r14_auth_mt (id int);
GRANT MAINTAIN ON TABLE r14_auth_mt TO r14_auth_m1;

-- 1. has_table_privilege for MAINTAIN
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT has_table_privilege('r14_auth_m1','r14_auth_mt','MAINTAIN')::text AS ok;

-- ============================================================================
-- SECTION B: GRANT SET/ALTER SYSTEM ON PARAMETER (PG 15+)
-- ============================================================================

CREATE ROLE r14_auth_p1;
GRANT SET ON PARAMETER search_path TO r14_auth_p1;

CREATE ROLE r14_auth_p2;
GRANT ALTER SYSTEM ON PARAMETER work_mem TO r14_auth_p2;

-- 2. has_parameter_privilege
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT has_parameter_privilege('r14_auth_p2','work_mem','ALTER SYSTEM')::text AS ok;

-- ============================================================================
-- SECTION C: Column-level GRANT / REVOKE
-- ============================================================================

CREATE ROLE r14_auth_c1;
CREATE TABLE r14_auth_ct (id int, secret text);
GRANT SELECT (id) ON r14_auth_ct TO r14_auth_c1;

-- 3. has_column_privilege on granted column
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT has_column_privilege('r14_auth_c1','r14_auth_ct','id','SELECT')::text AS ok;

-- 4. no privilege on other column
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT has_column_privilege('r14_auth_c1','r14_auth_ct','secret','SELECT')::text AS ok;

CREATE ROLE r14_auth_c3;
CREATE TABLE r14_auth_cr (id int, v int);
GRANT SELECT (id, v) ON r14_auth_cr TO r14_auth_c3;
REVOKE SELECT (v) ON r14_auth_cr FROM r14_auth_c3;

-- 5. revoke only affects named column
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT has_column_privilege('r14_auth_c3','r14_auth_cr','v','SELECT')::text AS ok;

-- ============================================================================
-- SECTION D: Role options
-- ============================================================================

CREATE ROLE r14_auth_cl WITH LOGIN CONNECTION LIMIT 5;

-- 6. CONNECTION LIMIT persisted
-- begin-expected
-- columns: cl
-- row: 5
-- end-expected
SELECT rolconnlimit::text AS cl FROM pg_roles WHERE rolname = 'r14_auth_cl';

CREATE ROLE r14_auth_ni WITH NOINHERIT;

-- 7. NOINHERIT flag
-- begin-expected
-- columns: i
-- row: f
-- end-expected
SELECT rolinherit::text AS i FROM pg_roles WHERE rolname = 'r14_auth_ni';

CREATE ROLE r14_auth_rep WITH REPLICATION;

-- 8. REPLICATION role attribute
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT rolreplication::text AS r FROM pg_roles WHERE rolname = 'r14_auth_rep';

-- ============================================================================
-- SECTION E: GRANT WITH ADMIN / GRANT OPTION
-- ============================================================================

CREATE ROLE r14_auth_a1;
CREATE ROLE r14_auth_a2;
GRANT r14_auth_a1 TO r14_auth_a2 WITH ADMIN OPTION;

-- 9. admin_option flag in pg_auth_members
-- begin-expected
-- columns: ao
-- row: t
-- end-expected
SELECT admin_option::text AS ao
  FROM pg_auth_members m
  JOIN pg_roles r ON m.roleid = r.oid
  JOIN pg_roles m2 ON m.member = m2.oid
  WHERE r.rolname = 'r14_auth_a1' AND m2.rolname = 'r14_auth_a2';

CREATE ROLE r14_auth_go1;
CREATE TABLE r14_auth_got (id int);
GRANT SELECT ON r14_auth_got TO r14_auth_go1 WITH GRANT OPTION;

-- 10. WITH GRANT OPTION visible via has_table_privilege
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT has_table_privilege('r14_auth_go1','r14_auth_got','SELECT WITH GRANT OPTION')::text AS ok;

-- ============================================================================
-- SECTION F: SECURITY DEFINER
-- ============================================================================

CREATE FUNCTION r14_auth_sd() RETURNS int AS 'SELECT 1' LANGUAGE SQL SECURITY DEFINER;

-- 11. prosecdef flag
-- begin-expected
-- columns: sd
-- row: t
-- end-expected
SELECT prosecdef::text AS sd FROM pg_proc WHERE proname = 'r14_auth_sd';

CREATE FUNCTION r14_auth_sd2() RETURNS int AS 'SELECT 1'
  LANGUAGE SQL SECURITY DEFINER SET search_path = pg_catalog;

-- 12. proconfig contains search_path
-- begin-expected
-- columns: has_sp
-- row: t
-- end-expected
SELECT (proconfig::text LIKE '%search_path%')::text AS has_sp
  FROM pg_proc WHERE proname = 'r14_auth_sd2';

-- ============================================================================
-- SECTION G: DEFAULT PRIVILEGES
-- ============================================================================

CREATE ROLE r14_auth_dp;
ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO r14_auth_dp;

-- 13. pg_default_acl has at least one row
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM pg_default_acl;

-- ============================================================================
-- SECTION H: has_* helpers
-- ============================================================================

-- 14. has_database_privilege on current db
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT has_database_privilege(current_database(),'CONNECT')::text AS ok;

CREATE ROLE r14_auth_hr1;
CREATE ROLE r14_auth_hr2;
GRANT r14_auth_hr1 TO r14_auth_hr2;

-- 15. pg_has_role
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT pg_has_role('r14_auth_hr2','r14_auth_hr1','USAGE')::text AS ok;
