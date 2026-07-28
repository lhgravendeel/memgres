-- ============================================================================
-- Feature Comparison: validation of programmable object definitions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- CREATE AGGREGATE / OPERATOR / CAST / EVENT TRIGGER / STATISTICS all name
-- other objects: a transition function, a cast function, a trigger function,
-- a table's columns. PostgreSQL resolves every one of those at definition time
-- and refuses a definition that could never work. This file covers those
-- refusals, plus the LANGUAGE check, RETURNS void, and the signature that
-- CREATE OR REPLACE FUNCTION is not allowed to change.
-- ============================================================================

-- ============================================================================
-- Setup: remove anything a previous run left behind
-- ============================================================================

DROP EVENT TRIGGER IF EXISTS dpo_et7;
DROP STATISTICS IF EXISTS dpo_st7;
DROP CAST IF EXISTS (int AS dpo_ct);
DROP OPERATOR IF EXISTS ##= (int, int);
DROP AGGREGATE IF EXISTS dpo_mysum(int);
DROP VIEW IF EXISTS dpo_v CASCADE;
DROP TABLE IF EXISTS dpo_t CASCADE;
DROP FUNCTION IF EXISTS dpo_sfunc(int, int) CASCADE;
DROP FUNCTION IF EXISTS dpo_cf(int) CASCADE;
DROP FUNCTION IF EXISTS dpo_etf() CASCADE;
DROP FUNCTION IF EXISTS dpo_etf2() CASCADE;
DROP FUNCTION IF EXISTS dpo_vf() CASCADE;
DROP FUNCTION IF EXISTS dpo_rf(int) CASCADE;
DROP FUNCTION IF EXISTS dpo_rf2(int) CASCADE;
DROP TYPE IF EXISTS dpo_ct CASCADE;

CREATE FUNCTION dpo_sfunc(int, int) RETURNS int LANGUAGE sql AS $$ SELECT $1 + $2 $$;

-- ============================================================================
-- CREATE AGGREGATE
-- ============================================================================

-- No state function: the aggregate could not compute anything.
CREATE AGGREGATE dpo_agg1(int) (STYPE = int);

-- No state type.
CREATE AGGREGATE dpo_agg2(int) (SFUNC = dpo_sfunc);

-- The transition function is looked up as sfunc(stype, aggregated args).
CREATE AGGREGATE dpo_agg3(text) (SFUNC = dpo_sfunc, STYPE = int);

-- An ordered-set aggregate passes only its post-ORDER BY arguments to sfunc.
CREATE AGGREGATE dpo_os(float8 ORDER BY float8)
  (SFUNC = dpo_sfunc, STYPE = int, FINALFUNC = dpo_nofinal);

-- A well-formed aggregate is accepted and is callable.
CREATE AGGREGATE dpo_mysum(int) (SFUNC = dpo_sfunc, STYPE = int, INITCOND = '0');

-- begin-expected
-- columns: dpo_mysum
-- row: 1
-- end-expected
SELECT dpo_mysum(1);

-- begin-expected
-- columns: dpo_mysum
-- row: 3
-- end-expected
SELECT dpo_mysum(x) FROM (VALUES (1),(2)) v(x);

-- Same name and argument types as the one just created.
CREATE AGGREGATE dpo_mysum(int) (SFUNC = dpo_sfunc, STYPE = int);

-- DROP and ALTER name an aggregate by its signature, not by name alone.
DROP AGGREGATE dpo_mysum(text);

ALTER AGGREGATE dpo_nosuch(int) RENAME TO dpo_x;

DROP AGGREGATE dpo_mysum(int);

-- ============================================================================
-- CREATE OPERATOR
-- ============================================================================

-- No FUNCTION: nothing to evaluate.
CREATE OPERATOR ===== (LEFTARG = int, RIGHTARG = int);

-- "ab" is a word, and only a symbolic operator name is grammatical here.
CREATE OPERATOR ab (LEFTARG = int, RIGHTARG = int, FUNCTION = dpo_sfunc);

-- The function is resolved against the declared argument types.
CREATE OPERATOR ##= (LEFTARG = int, RIGHTARG = int, FUNCTION = dpo_nosuch);

CREATE OPERATOR ##= (LEFTARG = int, RIGHTARG = int, FUNCTION = dpo_sfunc);

DROP OPERATOR ##= (int, int);

-- ============================================================================
-- CREATE CAST
-- ============================================================================

CREATE TYPE dpo_ct AS (x int);
CREATE FUNCTION dpo_cf(int) RETURNS dpo_ct LANGUAGE sql AS $$ SELECT ROW($1)::dpo_ct $$;

CREATE CAST (int AS dpo_ct) WITH FUNCTION dpo_cf(int);

-- Registering the same source/target pair twice.
CREATE CAST (int AS dpo_ct) WITH FUNCTION dpo_cf(int);

-- The cast function has to accept the source type ...
CREATE CAST (text AS dpo_ct) WITH FUNCTION dpo_cf(int);

-- ... and produce the target type.
CREATE CAST (int AS int) WITH FUNCTION dpo_cf(int);

-- An unknown cast function.
CREATE CAST (bigint AS dpo_ct) WITH FUNCTION dpo_nosuch(bigint);

-- WITHOUT FUNCTION claims the two types are the same bytes; these are not.
CREATE CAST (int AS text) WITHOUT FUNCTION;

CREATE CAST (int AS int) WITHOUT FUNCTION;

DROP CAST (int AS dpo_ct);

DROP CAST (int AS dpo_ct);

-- ============================================================================
-- CREATE EVENT TRIGGER
-- ============================================================================

CREATE FUNCTION dpo_etf() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$;
CREATE FUNCTION dpo_etf2() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$;

-- The trigger function must return event_trigger.
CREATE EVENT TRIGGER dpo_et1 ON ddl_command_start EXECUTE FUNCTION dpo_etf();

-- TAG is the only filter variable there is.
CREATE EVENT TRIGGER dpo_et2 ON ddl_command_start
  WHEN NOSUCHVAR IN ('CREATE TABLE') EXECUTE FUNCTION dpo_etf2();

-- SELECT is a real command tag, but not one event triggers fire for.
CREATE EVENT TRIGGER dpo_et3 ON ddl_command_start
  WHEN TAG IN ('SELECT') EXECUTE FUNCTION dpo_etf2();

CREATE EVENT TRIGGER dpo_et4 ON nosuchevent EXECUTE FUNCTION dpo_etf2();

CREATE EVENT TRIGGER dpo_et7 ON ddl_command_end EXECUTE FUNCTION dpo_etf2();

CREATE EVENT TRIGGER dpo_et7 ON ddl_command_end EXECUTE FUNCTION dpo_etf2();

DROP EVENT TRIGGER dpo_et7;

DROP EVENT TRIGGER dpo_nosuch;

-- ============================================================================
-- CREATE STATISTICS
-- ============================================================================

CREATE TABLE dpo_t(a int, b int);
CREATE VIEW dpo_v AS SELECT * FROM dpo_t;

-- A single column has no cross-column correlation to record.
CREATE STATISTICS dpo_st1 ON a FROM dpo_t;

CREATE STATISTICS dpo_st2 ON a, nosuchcol FROM dpo_t;

CREATE STATISTICS dpo_st3 (nosuchkind) ON a, b FROM dpo_t;

CREATE STATISTICS dpo_st4 ON a, a FROM dpo_t;

CREATE STATISTICS dpo_st5 ON a, b FROM dpo_nosuchtable;

-- A view has no stored rows to sample.
CREATE STATISTICS dpo_st6 ON a, b FROM dpo_v;

CREATE STATISTICS dpo_st7 (ndistinct) ON a, b FROM dpo_t;

CREATE STATISTICS dpo_st7 (ndistinct) ON a, b FROM dpo_t;

ALTER STATISTICS dpo_nosuch RENAME TO dpo_x;

DROP STATISTICS dpo_nosuch;

DROP STATISTICS dpo_st7;

-- ============================================================================
-- LANGUAGE, RETURNS void, and CREATE OR REPLACE FUNCTION
-- ============================================================================

CREATE FUNCTION dpo_f1() RETURNS int LANGUAGE nosuchlang AS $$ x $$;

-- Calling something and discarding the value is how a void SQL function is written.
CREATE FUNCTION dpo_vf() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected
-- columns: is_null
-- row: t
-- end-expected
SELECT dpo_vf() IS NULL AS is_null;

CREATE FUNCTION dpo_rf(p int) RETURNS text LANGUAGE sql AS $$ SELECT 'x' $$;

-- Replacing may not change the result type a caller compiled against ...
CREATE OR REPLACE FUNCTION dpo_rf(p int) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;

-- ... nor the name of an input parameter a caller may pass by name.
CREATE OR REPLACE FUNCTION dpo_rf(q int) RETURNS text LANGUAGE sql AS $$ SELECT 'y' $$;

-- Replacing only the body is fine.
CREATE OR REPLACE FUNCTION dpo_rf(p int) RETURNS text LANGUAGE sql AS $$ SELECT 'z' $$;

-- begin-expected
-- columns: dpo_rf
-- row: z
-- end-expected
SELECT dpo_rf(1);

CREATE FUNCTION dpo_rf2(p int, OUT o1 int, OUT o2 int) RETURNS record
  LANGUAGE sql AS $$ SELECT 1, 2 $$;

-- OUT parameters name the columns of the result record, so renaming one changes
-- the return type.
CREATE OR REPLACE FUNCTION dpo_rf2(p int, OUT o1 int, OUT ox int) RETURNS record
  LANGUAGE sql AS $$ SELECT 1, 2 $$;

-- ============================================================================
-- Teardown
-- ============================================================================

DROP VIEW IF EXISTS dpo_v CASCADE;
DROP TABLE IF EXISTS dpo_t CASCADE;
DROP FUNCTION IF EXISTS dpo_sfunc(int, int) CASCADE;
DROP FUNCTION IF EXISTS dpo_cf(int) CASCADE;
DROP FUNCTION IF EXISTS dpo_etf() CASCADE;
DROP FUNCTION IF EXISTS dpo_etf2() CASCADE;
DROP FUNCTION IF EXISTS dpo_vf() CASCADE;
DROP FUNCTION IF EXISTS dpo_rf(int) CASCADE;
DROP FUNCTION IF EXISTS dpo_rf2(int) CASCADE;
DROP TYPE IF EXISTS dpo_ct CASCADE;
