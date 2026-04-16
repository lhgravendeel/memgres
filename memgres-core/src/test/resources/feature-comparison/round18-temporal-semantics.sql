-- ============================================================================
-- Feature Comparison: Round 18 — Temporal / string semantic-depth gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION Y1: to_char TZH / TZM / tz
-- ============================================================================

-- 1. TZH returns signed two-digit hours at UTC
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (to_char('2025-01-01 12:00:00+00'::timestamptz AT TIME ZONE 'UTC', 'TZH') ~ '^[-+][0-9]{2}$') AS ok;

-- 2. TZM returns two-digit minutes
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (to_char('2025-01-01 12:00:00+05:30'::timestamptz, 'TZM') ~ '^[0-9]{2}$') AS ok;

-- 3. lowercase tz pattern produces lowercase abbrev
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (to_char('2025-01-01 12:00:00+00'::timestamptz, 'tz') = lower(to_char('2025-01-01 12:00:00+00'::timestamptz, 'tz'))) AS ok;

-- ============================================================================
-- SECTION Y2: extract(julian from ts)
-- ============================================================================

-- 4. julian day for 2000-01-01 UTC starts with 2451544
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (extract(julian from '2000-01-01 00:00:00+00'::timestamptz)::text LIKE '2451544%') AS ok;

-- ============================================================================
-- SECTION Y3: extract(epoch from interval)
-- ============================================================================

-- 5. 1 year 1 second → 31557601 in epoch
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (extract(epoch from interval '1 year 1 second')::text LIKE '%31557601%') AS ok;

-- ============================================================================
-- SECTION Y4: age() preserves hours
-- ============================================================================

-- 6. hour diff = 6
-- begin-expected
-- columns: h
-- row: 6
-- end-expected
SELECT extract(hour from age('2020-01-02 12:30:00'::timestamp, '2020-01-01 06:00:00'::timestamp))::int AS h;

-- ============================================================================
-- SECTION Y5: isfinite(interval)
-- ============================================================================

-- 7. isfinite(interval '1 day') = true
-- begin-expected
-- columns: f
-- row: t
-- end-expected
SELECT isfinite(interval '1 day') AS f;

-- ============================================================================
-- SECTION Z1: date_bin small diff returns origin
-- ============================================================================

-- 8. date_bin(1 hour, origin+10min, origin) = origin
-- begin-expected
-- columns: ts
-- row: 2025-01-01 00:00:00
-- end-expected
SELECT date_bin('1 hour'::interval, '2025-01-01 00:10:00'::timestamp, '2025-01-01 00:00:00'::timestamp)::text AS ts;

-- ============================================================================
-- SECTION Z2: timezone(text, ts) functional form
-- ============================================================================

-- 9. timezone('UTC', ts@+02) shifts to UTC
-- begin-expected
-- columns: ts
-- row: 2025-01-01 10:00:00
-- end-expected
SELECT timezone('UTC', '2025-01-01 12:00:00+02'::timestamptz)::text AS ts;

-- ============================================================================
-- SECTION Z3: pg_sleep_for / pg_sleep_until
-- ============================================================================

-- 10. pg_sleep_for registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_sleep_for';

-- 11. pg_sleep_until registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_sleep_until';

-- ============================================================================
-- SECTION Z4: overlay(bytea)
-- ============================================================================

-- 12. overlay(bytea placing ... from 1) replaces first bytes
-- begin-expected
-- columns: v
-- row: xxcdef
-- end-expected
SELECT encode(overlay('abcdef'::bytea placing 'xx'::bytea from 1), 'escape') AS v;

-- ============================================================================
-- SECTION Z5: left / right negative arg
-- ============================================================================

-- 13. left('abcdef', -2) = 'abcd'
-- begin-expected
-- columns: v
-- row: abcd
-- end-expected
SELECT left('abcdef', -2) AS v;

-- 14. right('abcdef', -2) = 'cdef'
-- begin-expected
-- columns: v
-- row: cdef
-- end-expected
SELECT right('abcdef', -2) AS v;
