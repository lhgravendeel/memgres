-- ============================================================================
-- Feature Comparison: Round 15 — Transaction + session controls
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_tx CASCADE;
CREATE SCHEMA r15_tx;
SET search_path = r15_tx, public;

-- ============================================================================
-- SECTION A: START TRANSACTION
-- ============================================================================

-- 1. Plain
START TRANSACTION;
ROLLBACK;

-- 2. With isolation level
START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- begin-expected
-- columns: transaction_isolation
-- row: serializable
-- end-expected
SHOW transaction_isolation;
ROLLBACK;

-- 3. READ ONLY
START TRANSACTION READ ONLY;
-- begin-expected
-- columns: transaction_read_only
-- row: on
-- end-expected
SHOW transaction_read_only;
ROLLBACK;

-- ============================================================================
-- SECTION B: COMMIT/ROLLBACK AND NO CHAIN / AND CHAIN
-- ============================================================================

-- 4. COMMIT AND NO CHAIN
BEGIN;
COMMIT AND NO CHAIN;

-- 5. ROLLBACK AND NO CHAIN
BEGIN;
ROLLBACK AND NO CHAIN;

-- 6. COMMIT AND CHAIN starts new txn
CREATE TABLE r15_ch (id int);
BEGIN;
INSERT INTO r15_ch VALUES (1);
COMMIT AND CHAIN;

INSERT INTO r15_ch VALUES (2);
ROLLBACK;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_ch;

-- ============================================================================
-- SECTION C: pg_export_snapshot + SET TRANSACTION SNAPSHOT
-- ============================================================================

BEGIN ISOLATION LEVEL REPEATABLE READ;
-- 7. pg_export_snapshot()
SELECT pg_export_snapshot();
ROLLBACK;

-- ============================================================================
-- SECTION D: Session authorization
-- ============================================================================

-- 8. RESET SESSION AUTHORIZATION
RESET SESSION AUTHORIZATION;

-- 9. SET LOCAL SESSION AUTHORIZATION (scoped to txn)
CREATE ROLE r15_sa_r1;
BEGIN;
SET LOCAL SESSION AUTHORIZATION r15_sa_r1;

-- begin-expected
-- columns: session_authorization
-- row: r15_sa_r1
-- end-expected
SHOW session_authorization;
ROLLBACK;

-- ============================================================================
-- SECTION E: Role switching
-- ============================================================================

CREATE ROLE r15_r_reset;
GRANT r15_r_reset TO CURRENT_USER;
SET ROLE r15_r_reset;

-- 10. current_role reflects SET ROLE
-- begin-expected
-- columns: r
-- row: r15_r_reset
-- end-expected
SELECT current_role AS r;

RESET ROLE;

-- 11. RESET ROLE reverts
CREATE ROLE r15_r_none;
GRANT r15_r_none TO CURRENT_USER;
SET ROLE r15_r_none;

-- begin-expected
-- columns: r
-- row: r15_r_none
-- end-expected
SELECT current_role AS r;

SET ROLE NONE;

-- 12. current_user also tracks SET ROLE
CREATE ROLE r15_r_cr;
GRANT r15_r_cr TO CURRENT_USER;
SET ROLE r15_r_cr;

-- begin-expected
-- columns: u
-- row: r15_r_cr
-- end-expected
SELECT current_user AS u;

RESET ROLE;

-- ============================================================================
-- SECTION F: information_schema role views
-- ============================================================================

-- 13. applicable_roles
SELECT count(*)::int FROM information_schema.applicable_roles;

-- 14. enabled_roles
SELECT count(*)::int FROM information_schema.enabled_roles;

-- 15. role_table_grants
CREATE TABLE r15_rtg (id int);
CREATE ROLE r15_rtg_r;
GRANT SELECT ON r15_rtg TO r15_rtg_r;

SELECT count(*)::int AS c FROM information_schema.role_table_grants
  WHERE grantee='r15_rtg_r' AND table_name='r15_rtg';

-- ============================================================================
-- SECTION G: SAVEPOINT within START TRANSACTION
-- ============================================================================

CREATE TABLE r15_sp (id int);
START TRANSACTION;
SAVEPOINT s1;
INSERT INTO r15_sp VALUES (1);
ROLLBACK TO SAVEPOINT s1;
COMMIT;

-- 16. INSERT rolled back
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM r15_sp;
