-- ============================================================================
-- Feature Comparison: Round 16 — Numeric / decimal edge cases
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r16_num CASCADE;
CREATE SCHEMA r16_num;
SET search_path = r16_num, public;

-- ============================================================================
-- SECTION D1: negative scale in numeric(p, -s)
-- ============================================================================

-- 1. numeric(5,-2) rounds 1234.56 to hundreds
-- begin-expected
-- columns: v
-- row: 1200
-- end-expected
SELECT 1234.56::numeric(5,-2) AS v;

-- ============================================================================
-- SECTION D2: NaN equality / ordering
-- ============================================================================

-- 2. NaN = NaN is TRUE for numeric
-- begin-expected
-- columns: eq
-- row: t
-- end-expected
SELECT ('NaN'::numeric = 'NaN'::numeric) AS eq;

-- 3. NaN orders greatest
-- begin-expected
-- columns: gt
-- row: t
-- end-expected
SELECT ('NaN'::numeric > 1e308::numeric) AS gt;

-- ============================================================================
-- SECTION D3: information_schema.columns numeric_precision / scale
-- ============================================================================

CREATE TABLE r16_num_typmod (a numeric(10,2), b numeric(5));

-- 4. numeric(10,2) → precision 10, scale 2
-- begin-expected
-- columns: column_name, numeric_precision, numeric_scale
-- row: a, 10, 2
-- row: b, 5, 0
-- end-expected
SELECT column_name, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_name = 'r16_num_typmod'
ORDER BY ordinal_position;

-- ============================================================================
-- SECTION D4: width_bucket with array thresholds
-- ============================================================================

-- 5. width_bucket(15, ARRAY[10,20,30]) = 1
-- begin-expected
-- columns: b
-- row: 1
-- end-expected
SELECT width_bucket(15, ARRAY[10, 20, 30]) AS b;

-- ============================================================================
-- SECTION D5: money formatting has currency prefix
-- ============================================================================

-- 6. Money text output contains "$" (lc_monetary=C/en_US default)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (12.34::money::text LIKE '%$%') AS ok;

-- ============================================================================
-- SECTION D6: Float8 infinity parsing (case-insensitive)
-- ============================================================================

-- 7. 'inf'::float8 > 1e308
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('inf'::float8 > 1e308) AS ok;

-- 8. 'infinity'::float8
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('infinity'::float8 > 1e308) AS ok;

-- 9. '+Infinity'::float8
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('+Infinity'::float8 > 1e308) AS ok;

-- ============================================================================
-- SECTION D7: round / trunc negative precision (exact BigDecimal math)
-- ============================================================================

-- 10. round(1234.56, -2) = 1200 exactly
-- begin-expected
-- columns: v
-- row: 1200
-- end-expected
SELECT round(1234.56, -2) AS v;

-- 11. trunc(1234.56, -2) = 1200 exactly
-- begin-expected
-- columns: v
-- row: 1200
-- end-expected
SELECT trunc(1234.56, -2) AS v;

-- ============================================================================
-- SECTION D8: mod on numeric
-- ============================================================================

-- 12. mod(3.7, 2) = 1.7
-- begin-expected
-- columns: v
-- row: 1.7
-- end-expected
SELECT mod(3.7::numeric, 2::numeric) AS v;

-- ============================================================================
-- SECTION D9: lcm overflow → SQLSTATE 22003
-- ============================================================================

-- 13. lcm(BIGINT_MAX, 2) must error
-- begin-expected-error
-- sqlstate: 22003
-- message-like: out of range
-- end-expected-error
SELECT lcm(9223372036854775807::bigint, 2::bigint);

-- ============================================================================
-- SECTION D10: factorial overflow promotion
-- ============================================================================

-- 14. factorial(25) must be exact numeric 25!
-- begin-expected
-- columns: v
-- row: 15511210043330985984000000
-- end-expected
SELECT factorial(25) AS v;

-- ============================================================================
-- SECTION D11: random_normal (PG 16+)
-- ============================================================================

-- 15. random_normal(0,1) returns a finite double
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (random_normal(0, 1) IS NOT NULL) AS ok;
