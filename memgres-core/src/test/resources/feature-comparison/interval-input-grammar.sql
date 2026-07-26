-- ============================================================================
-- Feature Comparison: interval input grammar
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL's interval reader takes a sequence of signed, possibly fractional
-- quantities with their own units, mixed with bare HH:MM[:SS] time fields. That
-- grammar is also what PG itself emits, so anything it prints has to read back
-- in -- '-1 mons +3 days' being the shape that breaks a dump/restore.
-- ============================================================================

-- ============================================================================
-- 1. Bare time fields, with hours free to exceed a day
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 05:00:00
-- end-expected
SELECT (INTERVAL '5:00')::text AS a;

-- begin-expected
-- columns: a
-- row: 27:00:00
-- end-expected
SELECT (INTERVAL '27:00')::text AS a;

-- ============================================================================
-- 2. Fractional quantities spill into the next-smaller unit
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 1 year 6 mons
-- end-expected
SELECT (INTERVAL '1.5 years')::text AS a;

-- begin-expected
-- columns: a
-- row: 1 mon 15 days
-- end-expected
SELECT (INTERVAL '1.5 mons')::text AS a;

-- begin-expected
-- columns: a
-- row: 1 day 12:00:00
-- end-expected
SELECT (INTERVAL '1.5 days')::text AS a;

-- ============================================================================
-- 3. Per-field signs, which is how PG prints a mixed interval
-- ============================================================================

-- begin-expected
-- columns: a
-- row: -1 mons +3 days
-- end-expected
SELECT (INTERVAL '-1 mons +3 days')::text AS a;

-- begin-expected
-- columns: a
-- row: -1 days -02:00:00
-- end-expected
SELECT (INTERVAL '1 day 2 hours ago')::text AS a;

-- ============================================================================
-- 4. ISO 8601 durations may carry negative components
-- ============================================================================

-- begin-expected
-- columns: a
-- row: -06:00:00
-- end-expected
SELECT (INTERVAL 'PT-6H')::text AS a;

-- ============================================================================
-- 5. The wide units
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 100 years
-- end-expected
SELECT (INTERVAL '1 century')::text AS a;

-- begin-expected
-- columns: a
-- row: 1000 years
-- end-expected
SELECT (INTERVAL '1 millennium')::text AS a;

-- begin-expected
-- columns: a
-- row: 20 years
-- end-expected
SELECT (INTERVAL '2 decades')::text AS a;

-- ============================================================================
-- 6. Arithmetic on the parsed value
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 3 days 08:00:00
-- end-expected
SELECT (INTERVAL '10 days' / 3)::text AS a;

-- begin-expected
-- columns: a
-- row: -1 mons -2 days
-- end-expected
SELECT (- INTERVAL '1 mon 2 days')::text AS a;

-- begin-expected
-- columns: a
-- row: 2 days 17:00:00
-- end-expected
SELECT (INTERVAL '1 day 2 hours' * 2.5)::text AS a;

-- ============================================================================
-- 7. The SQL-standard shapes still read
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 1 year 2 mons
-- end-expected
SELECT (INTERVAL '1-2')::text AS a;

-- begin-expected
-- columns: a
-- row: 2 days 04:05:06
-- end-expected
SELECT (INTERVAL '2 04:05:06')::text AS a;

-- ============================================================================
-- 8. PG's traditional forms: the @ prefix and a bare leading dot
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 1 day
-- end-expected
SELECT (INTERVAL '@ 1 day')::text AS a;

-- begin-expected
-- columns: a
-- row: -1 days
-- end-expected
SELECT (INTERVAL '@ 1 day ago')::text AS a;

-- begin-expected
-- columns: a
-- row: 12:00:00
-- end-expected
SELECT (INTERVAL '.5 days')::text AS a;

-- begin-expected
-- columns: a
-- row: 00:15:00
-- end-expected
SELECT (INTERVAL '.25 hours')::text AS a;
