-- ============================================================================
-- Feature Comparison: Round 18 — PL/pgSQL language depth
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION AH1: PERFORM sets FOUND
-- ============================================================================

DROP TABLE IF EXISTS r18_pf;
CREATE TABLE r18_pf(a int);
INSERT INTO r18_pf VALUES (1);

DROP FUNCTION IF EXISTS r18_pf_fn();
CREATE FUNCTION r18_pf_fn() RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN PERFORM * FROM r18_pf WHERE a = 1; RETURN FOUND; END $$;

-- 1. FOUND = true after matching PERFORM
-- begin-expected
-- columns: r18_pf_fn
-- row: t
-- end-expected
SELECT r18_pf_fn();

DROP FUNCTION IF EXISTS r18_pf_fn_neg();
CREATE FUNCTION r18_pf_fn_neg() RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN PERFORM * FROM r18_pf WHERE a = 999; RETURN FOUND; END $$;

-- 2. FOUND = false after non-matching PERFORM
-- begin-expected
-- columns: r18_pf_fn_neg
-- row: f
-- end-expected
SELECT r18_pf_fn_neg();

-- ============================================================================
-- SECTION AH2: GET DIAGNOSTICS v = PG_CONTEXT
-- ============================================================================

DROP FUNCTION IF EXISTS r18_pc_fn();
CREATE FUNCTION r18_pc_fn() RETURNS text LANGUAGE plpgsql AS $$
DECLARE ctx text;
BEGIN GET DIAGNOSTICS ctx = PG_CONTEXT; RETURN ctx; END $$;

-- 3. PG_CONTEXT mentions the calling function
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (lower(r18_pc_fn()) LIKE '%r18_pc_fn%') AS ok;

-- ============================================================================
-- SECTION AH3: WHEN OTHERS captures SQLSTATE
-- ============================================================================

DROP FUNCTION IF EXISTS r18_wo_fn();
CREATE FUNCTION r18_wo_fn() RETURNS text LANGUAGE plpgsql AS $$
DECLARE s text;
BEGIN
  BEGIN PERFORM 1/0;
  EXCEPTION WHEN OTHERS THEN s := SQLSTATE; RETURN s; END;
END $$;

-- 4. SQLSTATE = 22012 (divide by zero)
-- begin-expected
-- columns: r18_wo_fn
-- row: 22012
-- end-expected
SELECT r18_wo_fn();

-- ============================================================================
-- SECTION AH4: ASSERT raises with message
-- ============================================================================

DROP FUNCTION IF EXISTS r18_as_fn();
CREATE FUNCTION r18_as_fn() RETURNS void LANGUAGE plpgsql AS $$
BEGIN ASSERT false, 'r18_assert_msg'; END $$;

-- 5. ASSERT surfaces SQLSTATE P0004 and message
-- begin-expected-error
-- sqlstate: P0004
-- message-like: r18_assert_msg
-- end-expected-error
SELECT r18_as_fn();

-- ============================================================================
-- SECTION AH5: %TYPE resolution
-- ============================================================================

DROP TABLE IF EXISTS r18_tt;
CREATE TABLE r18_tt(id bigint, label text);

DROP FUNCTION IF EXISTS r18_tt_fn();
CREATE FUNCTION r18_tt_fn() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v r18_tt.id%TYPE;
BEGIN v := 9223372036854775807; RETURN v::text; END $$;

-- 6. %TYPE = bigint allows max bigint value
-- begin-expected
-- columns: r18_tt_fn
-- row: 9223372036854775807
-- end-expected
SELECT r18_tt_fn();
