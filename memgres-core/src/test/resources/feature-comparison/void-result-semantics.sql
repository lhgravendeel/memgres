-- ============================================================================
-- Feature Comparison: a function declared to return void answers with void
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A void result is not a NULL. PostgreSQL sends an empty value of type void, so
-- a client reads back an empty string and IS NULL is false. memgres agreed for
-- the blocking advisory-lock functions -- they were listed by name where the
-- result type is decided -- and disagreed for every other void function: pg_sleep,
-- pg_sleep_for, pg_stat_clear_snapshot and pg_stat_reset answered SQL NULL, and
-- setseed, pg_notify and the stat functions declared their result text or unknown.
--
-- The list of names is now read off the signature table, which already carried
-- prorettype 2278 for all of them, so the two cannot drift apart. Three advisory
-- functions turned out to be missing from that table altogether, which is why
-- they are asserted here alongside the rest.
-- ============================================================================

-- ============================================================================
-- 1. The value is an empty string, not NULL
-- ============================================================================
-- begin-expected
-- columns: v, isnull
-- row: , false
-- end-expected
SELECT pg_sleep(0)::text AS v, (pg_sleep(0) IS NULL)::text AS isnull;

-- begin-expected
-- columns: v, isnull
-- row: , false
-- end-expected
SELECT pg_stat_clear_snapshot()::text AS v,
       (pg_stat_clear_snapshot() IS NULL)::text AS isnull;

-- begin-expected
-- columns: v, isnull
-- row: , false
-- end-expected
SELECT pg_stat_reset()::text AS v, (pg_stat_reset() IS NULL)::text AS isnull;

-- begin-expected
-- columns: v, isnull
-- row: , false
-- end-expected
SELECT setseed(0.5)::text AS v, (setseed(0.25) IS NULL)::text AS isnull;

-- begin-expected
-- columns: v, isnull
-- row: , false
-- end-expected
SELECT pg_notify('vrs_chan', 'x')::text AS v,
       (pg_notify('vrs_chan', 'y') IS NULL)::text AS isnull;

-- ============================================================================
-- 2. And the declared type is void
-- ============================================================================
-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_sleep(0))::text AS t;

-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(setseed(0.75))::text AS t;

-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_stat_clear_snapshot())::text AS t;

-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_notify('vrs_chan', 'z'))::text AS t;

-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_stat_reset())::text AS t;

-- ============================================================================
-- 3. The advisory-lock family, including the three the signature table lacked
-- ============================================================================
-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_advisory_lock(9391001))::text AS t;

-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_advisory_lock_shared(9391002))::text AS t;

-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_advisory_xact_lock_shared(9391003))::text AS t;

-- begin-expected
-- columns: t
-- row: void
-- end-expected
SELECT pg_typeof(pg_advisory_unlock_all())::text AS t;

-- ============================================================================
-- 4. A function that returns something is untouched by the rule
-- ============================================================================
-- pg_advisory_unlock and its shared form answer boolean, not void
-- begin-expected
-- columns: t
-- row: boolean
-- end-expected
SELECT pg_typeof(pg_advisory_unlock(9391099))::text AS t;

-- begin-expected
-- columns: t
-- row: boolean
-- end-expected
SELECT pg_typeof(pg_advisory_unlock_shared(9391098))::text AS t;

-- begin-expected
-- columns: t
-- row: boolean
-- end-expected
SELECT pg_typeof(pg_try_advisory_lock(9391004))::text AS t;

SELECT pg_advisory_unlock_all();

-- ============================================================================
-- 5. The catalog agrees about what these return
-- ============================================================================
-- begin-expected
-- columns: proname, rettype
-- row: pg_advisory_lock_shared, void
-- row: pg_advisory_unlock_all, void
-- row: pg_advisory_unlock_shared, boolean
-- row: pg_advisory_xact_lock_shared, void
-- end-expected
SELECT DISTINCT p.proname::text AS proname, format_type(p.prorettype, NULL)::text AS rettype
  FROM pg_proc p
 WHERE p.proname IN ('pg_advisory_lock_shared', 'pg_advisory_xact_lock_shared',
                     'pg_advisory_unlock_all', 'pg_advisory_unlock_shared')
 ORDER BY proname;
