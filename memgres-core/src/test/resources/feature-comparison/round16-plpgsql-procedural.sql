-- ============================================================================
-- Feature Comparison: Round 16 — PL/pgSQL procedural surface fidelity
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r16_pl CASCADE;
CREATE SCHEMA r16_pl;
SET search_path = r16_pl, public;

-- ============================================================================
-- SECTION A1: RAISE levels and wire routing
-- ============================================================================

-- 1. RAISE DEBUG reaches the client when client_min_messages = debug1
SET client_min_messages = debug1;

CREATE OR REPLACE FUNCTION r16_raise_debug() RETURNS void AS $$
BEGIN RAISE DEBUG 'debug-payload-xyzzy'; END;
$$ LANGUAGE plpgsql;

SELECT r16_raise_debug();  -- PG 18: emits DEBUG; Memgres: silent

SET client_min_messages = notice;

-- 2. RAISE '%%' must collapse to a literal '%'
CREATE OR REPLACE FUNCTION r16_raise_pct() RETURNS void AS $$
BEGIN RAISE NOTICE '100%%'; END;
$$ LANGUAGE plpgsql;
SELECT r16_raise_pct();  -- PG 18 NOTICE text is "100%"; Memgres keeps "100%%"

-- 3. RAISE with extra positional args must error 42601
-- begin-expected-error
-- sqlstate: 42601
-- message-like: too many parameters specified for RAISE
-- end-expected-error
CREATE OR REPLACE FUNCTION r16_raise_extra() RETURNS void AS $$
BEGIN RAISE NOTICE 'no-placeholder', 'leftover-arg'; END;
$$ LANGUAGE plpgsql;
SELECT r16_raise_extra();

-- ============================================================================
-- SECTION A2: Bare RAISE; re-raise preserves auxiliary fields
-- ============================================================================

-- 4. COLUMN/CONSTRAINT/DATATYPE/TABLE/SCHEMA survive `RAISE;`
CREATE OR REPLACE FUNCTION r16_reraise_probe(OUT col text, OUT cns text,
                                              OUT dt text, OUT tbl text, OUT sch text)
AS $$
BEGIN
  BEGIN
    BEGIN
      RAISE EXCEPTION 'boom' USING
        COLUMN = 'c1', CONSTRAINT = 'k1', DATATYPE = 'int4',
        TABLE = 't1', SCHEMA = 's1';
    EXCEPTION WHEN OTHERS THEN
      RAISE;
    END;
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
      col = COLUMN_NAME,
      cns = CONSTRAINT_NAME,
      dt  = PG_DATATYPE_NAME,
      tbl = TABLE_NAME,
      sch = SCHEMA_NAME;
  END;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM r16_reraise_probe();

-- ============================================================================
-- SECTION A3: GET DIAGNOSTICS RESULT_OID
-- ============================================================================

-- 5. RESULT_OID is populated (non-null) after an INSERT
CREATE TABLE r16_roid (id int);
CREATE OR REPLACE FUNCTION r16_fn_roid() RETURNS oid AS $$
DECLARE v_oid oid;
BEGIN
  INSERT INTO r16_roid VALUES (1);
  GET DIAGNOSTICS v_oid = RESULT_OID;
  RETURN v_oid;
END;
$$ LANGUAGE plpgsql;
SELECT (r16_fn_roid() IS NOT NULL)::text AS nonnull;
-- PG 18: returns 't' (oid of last inserted row); Memgres: returns 'f'

-- ============================================================================
-- SECTION A4: PG_EXCEPTION_CONTEXT — real stack, not a fixed literal
-- ============================================================================

CREATE OR REPLACE FUNCTION r16_ctx_inner() RETURNS void AS $$
BEGIN RAISE EXCEPTION 'inner-boom'; END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION r16_ctx_outer(OUT ctx text) AS $$
BEGIN
  BEGIN
    PERFORM r16_ctx_inner();
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS ctx = PG_EXCEPTION_CONTEXT;
  END;
END;
$$ LANGUAGE plpgsql;

-- 6. Context must name r16_ctx_inner and include a line number
SELECT (ctx LIKE '%r16_ctx_inner%line %')::text AS ok FROM r16_ctx_outer();
-- PG 18: 't'; Memgres: 'f' (emits fixed "PL/pgSQL function")

-- ============================================================================
-- SECTION A5: client_min_messages filter on NOTICE
-- ============================================================================

-- 7. NOTICE is suppressed when client_min_messages = warning
SET client_min_messages = warning;
CREATE OR REPLACE FUNCTION r16_notice_only() RETURNS void AS $$
BEGIN RAISE NOTICE 'quiet-notice-abcdef'; END;
$$ LANGUAGE plpgsql;
SELECT r16_notice_only();  -- PG 18: no NOTICE; Memgres: NOTICE still delivered
SET client_min_messages = notice;
