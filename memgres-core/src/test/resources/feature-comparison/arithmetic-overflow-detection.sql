-- ============================================================================
-- Feature Comparison: arithmetic overflow is reported, not wrapped
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Intervals, bigints and network addresses all have a finite range. When a
-- result leaves it, PostgreSQL raises rather than silently wrapping round --
-- which is what turns an overflow into corrupt data.
-- ============================================================================

-- ============================================================================
-- 1. Interval field overflow
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT (INTERVAL '2147483647 months' + INTERVAL '1 month');

-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT (INTERVAL '100000000 years' * 1000);

-- Ordinary interval arithmetic is untouched
-- begin-expected
-- columns: a
-- row: 2 days
-- end-expected
SELECT (INTERVAL '1 day' + INTERVAL '1 day')::text AS a;

-- begin-expected
-- columns: a
-- row: 3 days 08:00:00
-- end-expected
SELECT (INTERVAL '10 days' / 3)::text AS a;

-- An infinite interval is not an overflow
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (INTERVAL 'infinity' + INTERVAL '1 day')::text AS a;

-- ============================================================================
-- 2. Integer division has one overflow
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT (-9223372036854775808)::bigint / (-1)::bigint;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT (10::bigint / 3::bigint)::text AS a;

-- ============================================================================
-- 3. The address space does not wrap
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: result is out of range
-- end-expected-error
SELECT ('255.255.255.255'::inet + 1);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: result is out of range
-- end-expected-error
SELECT ('0.0.0.0'::inet - 1);

-- begin-expected
-- columns: a
-- row: 255.255.255.255/32
-- end-expected
SELECT ('255.255.255.255'::inet + 0)::text AS a;

-- begin-expected
-- columns: a
-- row: 10.0.0.6/32
-- end-expected
SELECT ('10.0.0.1'::inet + 5)::text AS a;
