-- ============================================================================
-- Feature Comparison: Round 16 — CREATE / ALTER EXTENSION
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION P1: CREATE EXTENSION ... SCHEMA
-- ============================================================================

DROP EXTENSION IF EXISTS pgcrypto CASCADE;
CREATE SCHEMA IF NOT EXISTS r16_ext_s;
CREATE EXTENSION pgcrypto WITH SCHEMA r16_ext_s;

-- 1. pg_extension.extnamespace = r16_ext_s
-- begin-expected
-- columns: nsp
-- row: r16_ext_s
-- end-expected
SELECT n.nspname AS nsp FROM pg_extension e
JOIN pg_namespace n ON e.extnamespace = n.oid
WHERE e.extname='pgcrypto';

-- ============================================================================
-- SECTION P2: CREATE EXTENSION ... VERSION
-- ============================================================================

DROP EXTENSION IF EXISTS pgcrypto CASCADE;
CREATE EXTENSION pgcrypto VERSION '1.3';

-- 2. pg_extension.extversion = '1.3'
-- begin-expected
-- columns: v
-- row: 1.3
-- end-expected
SELECT extversion AS v FROM pg_extension WHERE extname='pgcrypto';

-- ============================================================================
-- SECTION P3: CREATE EXTENSION ... CASCADE
-- ============================================================================

DROP EXTENSION IF EXISTS pgcrypto CASCADE;
CREATE EXTENSION pgcrypto CASCADE;

-- 3. pgcrypto installed (CASCADE parses)
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_extension WHERE extname='pgcrypto';

-- ============================================================================
-- SECTION P4: ALTER EXTENSION UPDATE
-- ============================================================================

-- 4. ALTER EXTENSION UPDATE parses
-- begin-expected-noop
-- end-expected-noop
ALTER EXTENSION pgcrypto UPDATE;

-- ============================================================================
-- SECTION P5: ALTER EXTENSION SET SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS r16_ext_mv2;
ALTER EXTENSION pgcrypto SET SCHEMA r16_ext_mv2;

-- 5. pg_extension.extnamespace moved to r16_ext_mv2
-- begin-expected
-- columns: nsp
-- row: r16_ext_mv2
-- end-expected
SELECT n.nspname AS nsp FROM pg_extension e
JOIN pg_namespace n ON e.extnamespace = n.oid
WHERE e.extname='pgcrypto';
