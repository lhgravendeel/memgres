-- ============================================================================
-- Feature Comparison: Round 16 — Temporal edge cases
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION E1: date_trunc 3-arg timezone form (PG 12+)
-- ============================================================================

-- 1. date_trunc('day', ts, 'UTC')
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (date_trunc('day', '2025-06-15 23:30:00+00'::timestamptz, 'UTC')::text
          LIKE '2025-06-15%') AS ok;

-- ============================================================================
-- SECTION E2: INTERVAL field qualifiers
-- ============================================================================

-- 2. INTERVAL '1-3' YEAR TO MONTH → 1 year 3 mons
-- begin-expected
-- columns: v
-- row: 1 year 3 mons
-- end-expected
SELECT (INTERVAL '1-3' YEAR TO MONTH)::text AS v;

-- 3. INTERVAL '2 04:05:06' DAY TO SECOND → 2 days 04:05:06
-- begin-expected
-- columns: v
-- row: 2 days 04:05:06
-- end-expected
SELECT (INTERVAL '2 04:05:06' DAY TO SECOND)::text AS v;

-- ============================================================================
-- SECTION E3: to_char timezone pattern letters
-- ============================================================================

-- 4. to_char(ts, 'TZ') must not emit literal 'TZ'
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (to_char('2025-06-15 12:00:00+00'::timestamptz, 'TZ') <> 'TZ') AS ok;

-- 5. to_char(ts, 'OF') must not emit literal 'OF'
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (to_char('2025-06-15 12:00:00+00'::timestamptz, 'OF') <> 'OF') AS ok;

-- ============================================================================
-- SECTION E4: timestamp 'infinity' arithmetic
-- ============================================================================

-- 6. 'infinity'::timestamp + interval '1 day' = 'infinity'
-- begin-expected
-- columns: v
-- row: infinity
-- end-expected
SELECT ('infinity'::timestamp + interval '1 day')::text AS v;

-- ============================================================================
-- SECTION E5: extract(timezone / timezone_hour / timezone_minute)
-- ============================================================================

-- 7. extract(timezone FROM '+05' ts) = 18000 seconds
-- begin-expected
-- columns: v
-- row: 0
-- end-expected
SELECT extract(timezone FROM '2025-06-15 12:00:00+05'::timestamptz)::int AS v;

-- 8. extract(timezone_hour FROM '+05:30' ts) = 5
-- begin-expected
-- columns: v
-- row: 0
-- end-expected
SELECT extract(timezone_hour FROM '2025-06-15 12:00:00+05:30'::timestamptz)::int AS v;

-- 9. extract(timezone_minute FROM '+05:30' ts) = 30
-- begin-expected
-- columns: v
-- row: 0
-- end-expected
SELECT extract(timezone_minute FROM '2025-06-15 12:00:00+05:30'::timestamptz)::int AS v;

-- ============================================================================
-- SECTION E6: TIMETZ ordering and equality by UTC wall-clock
-- ============================================================================

-- 10. '12:00:00+05'::timetz = '07:00:00+00'::timetz
-- begin-expected
-- columns: eq
-- row: f
-- end-expected
SELECT ('12:00:00+05'::timetz = '07:00:00+00'::timetz) AS eq;

-- ============================================================================
-- SECTION E7: Year 0000 rejected
-- ============================================================================

-- 11. '0000-01-01'::date must error
-- begin-expected-error
-- message-like: out of range
-- end-expected-error
SELECT '0000-01-01'::date;
