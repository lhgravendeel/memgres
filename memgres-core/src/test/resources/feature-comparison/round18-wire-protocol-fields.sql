-- ============================================================================
-- Feature Comparison: Round 18 — Wire protocol ErrorResponse fields
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
--
-- These are wire-level observable fields on ErrorResponse/NoticeResponse.
-- SQL-level checks here are limited — the Java test is authoritative.

-- ============================================================================
-- SECTION AC1-AC7: errors expose V/C/H/F/L/R/P fields
-- ============================================================================

-- 1. Missing relation yields SQLSTATE 42P01 (C field)
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT * FROM pg_does_not_exist_r18;

-- 2. Syntax error yields 42601 with position (P field)
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT FROM;

-- 3. Undefined column with hint (H field)
DROP TABLE IF EXISTS r18_hint;
CREATE TABLE r18_hint(abc int);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: abc
-- end-expected-error
SELECT abcd FROM r18_hint;

-- ============================================================================
-- SECTION AC8: compute_query_id → pg_stat_activity.query_id non-null
-- ============================================================================

SET compute_query_id = on;

-- 4. query_id non-null and non-zero
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (query_id IS NOT NULL AND query_id <> 0) AS ok
  FROM pg_stat_activity WHERE pid = pg_backend_pid();
