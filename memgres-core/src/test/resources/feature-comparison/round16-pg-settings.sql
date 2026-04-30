-- ============================================================================
-- Feature Comparison: Round 16 — pg_settings completeness
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION O1: pg_settings.unit populated
-- ============================================================================

-- 1. work_mem has a non-null unit
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (unit IS NOT NULL AND unit <> '') AS ok
FROM pg_settings WHERE name='work_mem';

-- 2. statement_timeout unit = 'ms'
-- begin-expected
-- columns: u
-- row: ms
-- end-expected
SELECT unit AS u FROM pg_settings WHERE name='statement_timeout';

-- ============================================================================
-- SECTION O2: min_val / max_val populated
-- ============================================================================

-- 3. work_mem has min_val + max_val
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (min_val IS NOT NULL AND max_val IS NOT NULL) AS ok
FROM pg_settings WHERE name='work_mem';

-- ============================================================================
-- SECTION O3: enumvals populated
-- ============================================================================

-- 4. client_min_messages.enumvals is not null
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (enumvals IS NOT NULL) AS ok
FROM pg_settings WHERE name='client_min_messages';

-- ============================================================================
-- SECTION O4: boot_val ≠ setting after SET
-- ============================================================================

SET work_mem = '8MB';

-- 5. setting differs from boot_val after SET
-- begin-expected
-- columns: differs
-- row: t
-- end-expected
SELECT (setting <> boot_val) AS differs
FROM pg_settings WHERE name='work_mem';

RESET work_mem;
