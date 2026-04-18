-- ============================================================================
-- Feature Comparison: Round 13 — PL/pgSQL Feature Gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PL/pgSQL constructs that PG 18 accepts but Memgres does not.
-- ============================================================================

DROP SCHEMA IF EXISTS r13_pl CASCADE;
CREATE SCHEMA r13_pl;
SET search_path = r13_pl, public;

-- ============================================================================
-- SECTION A: CALL INTO (capture procedure OUT args)
-- ============================================================================

CREATE PROCEDURE r13_pl_proc(x int, OUT y int)
  LANGUAGE plpgsql AS $$ BEGIN y := x * 10; END $$;

CREATE FUNCTION r13_pl_call_into_fn() RETURNS int LANGUAGE plpgsql AS $$
DECLARE result int;
BEGIN
  CALL r13_pl_proc(7, result);
  RETURN result;
END $$;

-- 1. CALL INTO returns OUT arg
-- begin-expected
-- columns: r
-- row: 70
-- end-expected
SELECT r13_pl_call_into_fn()::text AS r;

-- ============================================================================
-- SECTION B: FOR IN EXECUTE
-- ============================================================================

CREATE FUNCTION r13_pl_for_exec() RETURNS int LANGUAGE plpgsql AS $$
DECLARE total int := 0; r record;
BEGIN
  FOR r IN EXECUTE 'SELECT generate_series(1,5) AS x' LOOP
    total := total + r.x;
  END LOOP;
  RETURN total;
END $$;

-- 2. FOR IN EXECUTE iterates dynamic query
-- begin-expected
-- columns: t
-- row: 15
-- end-expected
SELECT r13_pl_for_exec()::text AS t;

-- 3. FOR IN EXECUTE USING passes params
CREATE FUNCTION r13_pl_for_exec_using(lim int) RETURNS int LANGUAGE plpgsql AS $$
DECLARE total int := 0; r record;
BEGIN
  FOR r IN EXECUTE 'SELECT generate_series(1, $1) AS x' USING lim LOOP
    total := total + r.x;
  END LOOP;
  RETURN total;
END $$;

-- begin-expected
-- columns: t
-- row: 6
-- end-expected
SELECT r13_pl_for_exec_using(3)::text AS t;

-- ============================================================================
-- SECTION C: SELECT INTO STRICT error codes
-- ============================================================================

CREATE TABLE r13_pl_strict (id int);
INSERT INTO r13_pl_strict VALUES (1), (2);

CREATE FUNCTION r13_pl_strict_multi() RETURNS int LANGUAGE plpgsql AS $$
DECLARE v int;
BEGIN
  SELECT id INTO STRICT v FROM r13_pl_strict;
  RETURN v;
END $$;

-- 4. Multi-row STRICT → P0003
-- begin-expected-error
-- message-like: more than one row
-- end-expected-error
SELECT r13_pl_strict_multi();

CREATE TABLE r13_pl_strict0 (id int);

CREATE FUNCTION r13_pl_strict_zero() RETURNS int LANGUAGE plpgsql AS $$
DECLARE v int;
BEGIN
  SELECT id INTO STRICT v FROM r13_pl_strict0;
  RETURN v;
END $$;

-- 5. Zero-row STRICT → P0002
-- begin-expected-error
-- message-like: no rows
-- end-expected-error
SELECT r13_pl_strict_zero();

-- ============================================================================
-- SECTION D: FOREACH SLICE
-- ============================================================================

CREATE FUNCTION r13_pl_slice() RETURNS int LANGUAGE plpgsql AS $$
DECLARE row int[]; total int := 0;
BEGIN
  FOREACH row SLICE 1 IN ARRAY ARRAY[[1,2,3],[4,5,6]] LOOP
    total := total + row[1] + row[2] + row[3];
  END LOOP;
  RETURN total;
END $$;

-- 6. FOREACH SLICE 1
-- begin-expected
-- columns: t
-- row: 21
-- end-expected
SELECT r13_pl_slice()::text AS t;

-- ============================================================================
-- SECTION E: ASSERT
-- ============================================================================

CREATE FUNCTION r13_pl_assert_fn() RETURNS void LANGUAGE plpgsql AS $$
BEGIN ASSERT 1 = 2, 'custom msg r13'; END $$;

-- 7. ASSERT false raises with message
-- begin-expected-error
-- message-like: custom msg r13
-- end-expected-error
SELECT r13_pl_assert_fn();

-- ============================================================================
-- SECTION F: RETURN QUERY EXECUTE USING
-- ============================================================================

CREATE FUNCTION r13_pl_rq_exec(lim int) RETURNS SETOF int LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY EXECUTE 'SELECT generate_series(1, $1)' USING lim;
END $$;

-- 8. RETURN QUERY EXECUTE USING produces rows
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT count(*)::text AS c FROM r13_pl_rq_exec(3);

-- ============================================================================
-- SECTION G: GET STACKED DIAGNOSTICS extended fields
-- ============================================================================

CREATE FUNCTION r13_pl_gsd_ctx() RETURNS text LANGUAGE plpgsql AS $$
DECLARE ctx text;
BEGIN
  PERFORM 1/0;
EXCEPTION WHEN division_by_zero THEN
  GET STACKED DIAGNOSTICS ctx = PG_EXCEPTION_CONTEXT;
  RETURN ctx;
END $$;

-- 9. PG_EXCEPTION_CONTEXT populated
-- begin-expected
-- columns: has_ctx
-- row: t
-- end-expected
SELECT (r13_pl_gsd_ctx() IS NOT NULL AND length(r13_pl_gsd_ctx()) > 0)::text AS has_ctx;

-- ============================================================================
-- SECTION H: GET DIAGNOSTICS ROW_COUNT
-- ============================================================================

CREATE TABLE r13_pl_diag (id int);
INSERT INTO r13_pl_diag SELECT generate_series(1, 5);

CREATE FUNCTION r13_pl_diag_rc() RETURNS int LANGUAGE plpgsql AS $$
DECLARE c int;
BEGIN
  UPDATE r13_pl_diag SET id = id + 10;
  GET DIAGNOSTICS c = ROW_COUNT;
  RETURN c;
END $$;

-- 10. GET DIAGNOSTICS ROW_COUNT counts affected rows
-- begin-expected-error
-- sqlstate: 55000
-- message-like: replica identity
-- end-expected-error
SELECT r13_pl_diag_rc()::text AS rc;
