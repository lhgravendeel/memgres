-- ============================================================================
-- Feature Comparison: Round 16 — GRANT / REVOKE / DISCARD / SET
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION N1: GRANT ... GRANTED BY role
-- ============================================================================

DROP ROLE IF EXISTS r16_grantor;
DROP ROLE IF EXISTS r16_grantee;
CREATE ROLE r16_grantor;
CREATE ROLE r16_grantee;
DROP TABLE IF EXISTS r16_gt;
CREATE TABLE r16_gt (id int);

-- 1. GRANT ... GRANTED BY role parses
-- begin-expected-noop
-- end-expected-noop
GRANT SELECT ON r16_gt TO r16_grantee GRANTED BY r16_grantor;

-- ============================================================================
-- SECTION N2: GRANT ON LARGE OBJECT
-- ============================================================================

DROP ROLE IF EXISTS r16_lo_user;
CREATE ROLE r16_lo_user;
SELECT lo_create(12345);

-- 2. GRANT SELECT ON LARGE OBJECT loid parses
-- begin-expected-noop
-- end-expected-noop
GRANT SELECT ON LARGE OBJECT 12345 TO r16_lo_user;

-- ============================================================================
-- SECTION N3: GRANT ON FOREIGN SERVER
-- ============================================================================

DROP ROLE IF EXISTS r16_fs_u;
CREATE ROLE r16_fs_u;

-- 3. GRANT USAGE ON FOREIGN SERVER parses (error must NOT be 42601)
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
GRANT USAGE ON FOREIGN SERVER nonexistent TO r16_fs_u;

-- ============================================================================
-- SECTION N4: GRANT SET ON PARAMETER
-- ============================================================================

DROP ROLE IF EXISTS r16_set_u;
CREATE ROLE r16_set_u;

-- 4. GRANT SET ON PARAMETER parses
-- begin-expected-noop
-- end-expected-noop
GRANT SET ON PARAMETER work_mem TO r16_set_u;

-- ============================================================================
-- SECTION N5: ALTER DEFAULT PRIVILEGES FOR ROLE target
-- ============================================================================

DROP ROLE IF EXISTS r16_adp_owner;
DROP ROLE IF EXISTS r16_adp_grantee;
CREATE ROLE r16_adp_owner;
CREATE ROLE r16_adp_grantee;

ALTER DEFAULT PRIVILEGES FOR ROLE r16_adp_owner
    GRANT SELECT ON TABLES TO r16_adp_grantee;

-- 5. pg_default_acl.defaclrole = oid of r16_adp_owner
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n
FROM pg_default_acl da
JOIN pg_roles r ON da.defaclrole = r.oid
WHERE r.rolname='r16_adp_owner';

-- ============================================================================
-- SECTION N6: REVOKE ... CASCADE
-- ============================================================================

DROP ROLE IF EXISTS r16_rc_a;
DROP ROLE IF EXISTS r16_rc_b;
DROP TABLE IF EXISTS r16_rc_t;
CREATE ROLE r16_rc_a;
CREATE ROLE r16_rc_b;
CREATE TABLE r16_rc_t (id int);
GRANT SELECT ON r16_rc_t TO r16_rc_a WITH GRANT OPTION;
SET ROLE r16_rc_a;
GRANT SELECT ON r16_rc_t TO r16_rc_b;
RESET ROLE;
REVOKE SELECT ON r16_rc_t FROM r16_rc_a CASCADE;

-- 6. r16_rc_b lost SELECT via cascade
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT has_table_privilege('r16_rc_b','r16_rc_t','SELECT') AS ok;

-- ============================================================================
-- SECTION N7: DROP OWNED BY
-- ============================================================================

DROP ROLE IF EXISTS r16_do_u;
CREATE ROLE r16_do_u;
GRANT CREATE ON SCHEMA public TO r16_do_u;
SET ROLE r16_do_u;
CREATE TABLE r16_do_t (id int);
RESET ROLE;
DROP OWNED BY r16_do_u CASCADE;

-- 7. r16_do_t removed by DROP OWNED
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname='r16_do_t';

REVOKE CREATE ON SCHEMA public FROM r16_do_u;
DROP ROLE r16_do_u;

-- ============================================================================
-- SECTION N9: DISCARD SEQUENCES
-- ============================================================================

-- 8. DISCARD SEQUENCES parses
-- begin-expected-noop
-- end-expected-noop
DISCARD SEQUENCES;
