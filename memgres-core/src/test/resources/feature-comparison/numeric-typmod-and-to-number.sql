-- ============================================================================
-- Feature Comparison: numeric typmod enforcement and to_number/to_date input
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A numeric(p,s) is checked after rounding, so a value that rounds up out of
-- its precision overflows rather than silently widening the column. to_date
-- rejects an impossible day instead of clamping it, and to_number honours the
-- sign wherever the format puts it.
-- ============================================================================

-- ============================================================================
-- 1. numeric(p,s) is enforced after rounding
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 99.995::numeric(4,2);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 123::numeric(2,0);

-- A value that still fits after rounding is kept
-- begin-expected
-- columns: a
-- row: 99.99
-- end-expected
SELECT 99.994::numeric(4,2)::text AS a;

-- begin-expected
-- columns: a
-- row: 12.35
-- end-expected
SELECT 12.345::numeric(5,2)::text AS a;

-- ============================================================================
-- 2. to_date rejects an impossible day instead of clamping it
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range
-- end-expected-error
SELECT to_date('2026-02-30','YYYY-MM-DD');

-- begin-expected
-- columns: a
-- row: 2026-02-28
-- end-expected
SELECT to_date('2026-02-28','YYYY-MM-DD')::text AS a;

-- ============================================================================
-- 3. to_number honours a leading or trailing sign
-- ============================================================================

-- begin-expected
-- columns: a
-- row: -123
-- end-expected
SELECT to_number('123-','999S')::text AS a;

-- begin-expected
-- columns: a
-- row: -123
-- end-expected
SELECT to_number('-123','S999')::text AS a;

-- begin-expected
-- columns: a
-- row: 123
-- end-expected
SELECT to_number('123','999')::text AS a;
