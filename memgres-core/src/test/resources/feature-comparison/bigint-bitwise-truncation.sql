-- ============================================================================
-- Feature Comparison: Bigint bitwise/shift operators 64-bit precision
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG preserves full 64-bit precision for bigint bitwise operations.
-- Casting to int truncates to 32 bits, losing high bits.
-- ============================================================================

-- ============================================================================
-- 1. Left shift beyond 32 bits
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1099511627776
-- end-expected
SELECT 1::bigint << 40 AS result;

-- ============================================================================
-- 2. Right shift from high bits
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1048576
-- end-expected
SELECT (1::bigint << 40) >> 20 AS result;

-- ============================================================================
-- 3. Bitwise AND with 64-bit values
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1099511627776
-- end-expected
SELECT (1099511627776::bigint & 1099511627776::bigint) AS result;

-- ============================================================================
-- 4. Bitwise OR with 64-bit values
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1099511627777
-- end-expected
SELECT (1::bigint | (1::bigint << 40)) AS result;

-- ============================================================================
-- 5. Bitwise XOR with 64-bit values
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1099511627776
-- end-expected
SELECT (1099511627777::bigint # 1::bigint) AS result;
