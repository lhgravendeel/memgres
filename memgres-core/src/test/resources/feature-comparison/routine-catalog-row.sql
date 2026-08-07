-- ============================================================================
-- Feature Comparison: what a routine's catalog row says about it
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The pg_proc row is how a client finds out what a routine takes and what it
-- gives back. Most of it was wrong for anything beyond a function of plain
-- scalars returning one value.
--
-- SETOF was recorded as part of the return type rather than as the flag beside
-- it, so prorettype was 0 -- no type at all -- for every set-returning
-- function. An array-typed parameter or result was 0 for the same reason, and
-- so was one carrying a precision. pronargs counted the OUT parameters a call
-- does not pass. A function deriving its result from its OUT parameters
-- recorded a record whatever it really returned, and a procedure with an INOUT
-- parameter could not be created at all. proretset, prorows, provariadic and
-- pronargdefaults were left at zero for every routine.
--
-- prosqlbody is not compared: PostgreSQL keeps a SQL-standard body as a parsed
-- tree and prints the statements back from it, so its own deparsed text says
-- SELECT p AS p where memgres has the SELECT p it was given.
-- ============================================================================

SET search_path = public;

DROP FUNCTION IF EXISTS rcat_f(int) CASCADE;

DROP FUNCTION IF EXISTS rcat_f() CASCADE;

DROP PROCEDURE IF EXISTS rcat_p() CASCADE;

-- ============================================================================
-- What comes back, and whether it comes back a row at a time
-- ============================================================================

CREATE FUNCTION rcat_set(p int) RETURNS SETOF int LANGUAGE sql AS $$ SELECT p $$;

-- begin-expected
-- columns: r
-- row: integer|true|1000
-- end-expected
SELECT prorettype::regtype::text || '|' || proretset::text || '|' || prorows::text AS r
  FROM pg_proc WHERE proname = 'rcat_set';

-- begin-expected
-- columns: r
-- row: SETOF integer
-- end-expected
SELECT pg_get_function_result(oid) AS r FROM pg_proc WHERE proname = 'rcat_set';

CREATE FUNCTION rcat_rows(p int) RETURNS SETOF int LANGUAGE sql ROWS 7 AS $$ SELECT p $$;

-- begin-expected
-- columns: r
-- row: true|7
-- end-expected
SELECT proretset::text || '|' || prorows::text AS r FROM pg_proc WHERE proname = 'rcat_rows';

CREATE FUNCTION rcat_one(p int) RETURNS int LANGUAGE sql AS $$ SELECT p $$;

-- begin-expected
-- columns: r
-- row: integer|false|0
-- end-expected
SELECT prorettype::regtype::text || '|' || proretset::text || '|' || prorows::text AS r
  FROM pg_proc WHERE proname = 'rcat_one';

-- A result derived from OUT parameters is one of them, or a record when several.
CREATE FUNCTION rcat_out1(a int, OUT b int) LANGUAGE sql AS $$ SELECT a $$;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT prorettype::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_out1';

CREATE FUNCTION rcat_out2(a int, OUT b int, OUT c text) LANGUAGE sql AS $$ SELECT a, 'x' $$;

-- begin-expected
-- columns: r
-- row: record
-- end-expected
SELECT prorettype::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_out2';

CREATE FUNCTION rcat_inout(a int, INOUT b int) LANGUAGE sql AS $$ SELECT b $$;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT prorettype::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_inout';

-- RETURNS TABLE is a set of records whose columns are parameters of mode 't'.
CREATE FUNCTION rcat_tab() RETURNS TABLE(a int, b text) LANGUAGE sql AS $$ SELECT 1, 'x' $$;

-- begin-expected
-- columns: r
-- row: record|true|0|{t,t}
-- end-expected
SELECT prorettype::regtype::text || '|' || proretset::text || '|' || pronargs::text
    || '|' || proargmodes::text AS r FROM pg_proc WHERE proname = 'rcat_tab';

-- begin-expected
-- columns: r
-- row: TABLE(a integer, b text)
-- end-expected
SELECT pg_get_function_result(oid) AS r FROM pg_proc WHERE proname = 'rcat_tab';

-- A table column is a column of the result, so it is not among the arguments.
-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_tab';

-- A procedure gives nothing back, unless it has a parameter to give it back through.
CREATE PROCEDURE rcat_proc(a int) LANGUAGE sql AS $$ SELECT a $$;

-- begin-expected
-- columns: r
-- row: void
-- end-expected
SELECT prorettype::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_proc';

-- begin-expected
-- columns: r
-- row: <none>
-- end-expected
SELECT coalesce(pg_get_function_result(oid), '<none>') AS r FROM pg_proc WHERE proname = 'rcat_proc';

CREATE PROCEDURE rcat_pio(a int, INOUT b int) LANGUAGE sql AS $$ SELECT b $$;

-- begin-expected
-- columns: r
-- row: record
-- end-expected
SELECT prorettype::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_pio';

-- begin-expected
-- columns: r
-- row: IN a integer, INOUT b integer
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_pio';

-- ============================================================================
-- What a call passes
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 1|23|{23,23}|{i,o}
-- end-expected
SELECT pronargs::text || '|' || proargtypes::text || '|' || proallargtypes::text
    || '|' || proargmodes::text AS r FROM pg_proc WHERE proname = 'rcat_out1';

-- begin-expected
-- columns: r
-- row: 2|23 23|{i,b}
-- end-expected
SELECT pronargs::text || '|' || proargtypes::text || '|' || proargmodes::text AS r
  FROM pg_proc WHERE proname = 'rcat_inout';

-- The array columns say nothing, and are NULL, when every parameter is a plain input.
CREATE FUNCTION rcat_plain(a int, b text) RETURNS int LANGUAGE sql AS $$ SELECT a $$;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (proargmodes IS NULL AND proallargtypes IS NULL)::text AS r
  FROM pg_proc WHERE proname = 'rcat_plain';

-- begin-expected
-- columns: r
-- row: {a,b}
-- end-expected
SELECT proargnames::text AS r FROM pg_proc WHERE proname = 'rcat_plain';

-- An array of a type is a type of its own, and the row records that type.
CREATE FUNCTION rcat_arr(p int[]) RETURNS int[] LANGUAGE sql AS $$ SELECT p $$;

-- begin-expected
-- columns: r
-- row: 1007|1007
-- end-expected
SELECT proargtypes::text || '|' || prorettype::text AS r FROM pg_proc WHERE proname = 'rcat_arr';

-- begin-expected
-- columns: r
-- row: p integer[]
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_arr';

-- begin-expected
-- columns: r
-- row: integer[]
-- end-expected
SELECT pg_get_function_result(oid) AS r FROM pg_proc WHERE proname = 'rcat_arr';

-- pg_proc has nowhere to put a modifier, so the type it records is the bare type.
CREATE FUNCTION rcat_mod(p numeric(10,2), q varchar(5), r interval day to second(2))
  RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected
-- columns: r
-- row: 1700 1043 1186
-- end-expected
SELECT proargtypes::text AS r FROM pg_proc WHERE proname = 'rcat_mod';

-- begin-expected
-- columns: r
-- row: p numeric, q character varying, r interval
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_mod';

-- provariadic names the ELEMENT type the tail collects into, not the array itself.
CREATE FUNCTION rcat_var(VARIADIC c int[]) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT provariadic::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_var';

-- begin-expected
-- columns: r
-- row: VARIADIC c integer[]
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_var';

CREATE FUNCTION rcat_var2(a int, VARIADIC c text[]) RETURNS int LANGUAGE sql AS $$ SELECT a $$;

-- begin-expected
-- columns: r
-- row: text
-- end-expected
SELECT provariadic::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_var2';

-- A routine with no VARIADIC parameter has none, which reads as a dash.
-- begin-expected
-- columns: r
-- row: -
-- end-expected
SELECT provariadic::regtype::text AS r FROM pg_proc WHERE proname = 'rcat_one';

-- begin-expected
-- columns: r
-- row: -
-- end-expected
SELECT 0::oid::regtype::text AS r;

-- A default is counted, and printed where the arguments are printed with them.
CREATE FUNCTION rcat_def(a int, b int DEFAULT 3, c int DEFAULT 4) RETURNS int
  LANGUAGE sql AS $$ SELECT a $$;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT pronargdefaults::text AS r FROM pg_proc WHERE proname = 'rcat_def';

-- begin-expected
-- columns: r
-- row: a integer, b integer DEFAULT 3, c integer DEFAULT 4
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_def';

-- The identity form names a routine, and a default is no part of that.
-- begin-expected
-- columns: r
-- row: a integer, b integer, c integer
-- end-expected
SELECT pg_get_function_identity_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_def';

-- ============================================================================
-- What the row reads back as
-- ============================================================================

-- The identity form names the OUT parameters too.
-- begin-expected
-- columns: r
-- row: a integer, OUT b integer, OUT c text
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_out2';

-- begin-expected
-- columns: r
-- row: a integer, OUT b integer, OUT c text
-- end-expected
SELECT pg_get_function_identity_arguments(oid) AS r FROM pg_proc WHERE proname = 'rcat_out2';

-- A cost nobody wrote is 100 for a language the server has to interpret.
-- begin-expected
-- columns: r
-- row: 100
-- end-expected
SELECT procost::text AS r FROM pg_proc WHERE proname = 'rcat_one';

CREATE FUNCTION rcat_pl(p int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN p; END $$;

-- begin-expected
-- columns: r
-- row: 100
-- end-expected
SELECT procost::text AS r FROM pg_proc WHERE proname = 'rcat_pl';

CREATE FUNCTION rcat_attr(p int) RETURNS int LANGUAGE sql
  IMMUTABLE STRICT PARALLEL SAFE LEAKPROOF SECURITY DEFINER COST 42 AS $$ SELECT p $$;

-- begin-expected
-- columns: r
-- row: CREATE OR REPLACE FUNCTION public.rcat_attr(p integer) /  RETURNS integer /  LANGUAGE sql /  IMMUTABLE PARALLEL SAFE STRICT SECURITY DEFINER LEAKPROOF COST 42 / AS $function$ SELECT p $function$ / 
-- end-expected
SELECT replace(pg_get_functiondef(oid), chr(10), ' / ') AS r
  FROM pg_proc WHERE proname = 'rcat_attr';

-- ============================================================================
-- A signature that names its OUT parameters still finds the routine
-- ============================================================================

ALTER FUNCTION rcat_out1(a int, OUT b int) IMMUTABLE;

-- begin-expected
-- columns: r
-- row: i
-- end-expected
SELECT provolatile AS r FROM pg_proc WHERE proname = 'rcat_out1';

ALTER FUNCTION rcat_out1(int) STABLE;

-- begin-expected
-- columns: r
-- row: s
-- end-expected
SELECT provolatile AS r FROM pg_proc WHERE proname = 'rcat_out1';

DROP FUNCTION rcat_out1(a int, OUT b int);

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_proc WHERE proname = 'rcat_out1';

DROP FUNCTION IF EXISTS rcat_set(int) CASCADE;

DROP FUNCTION IF EXISTS rcat_rows(int) CASCADE;

DROP FUNCTION IF EXISTS rcat_one(int) CASCADE;

DROP FUNCTION IF EXISTS rcat_out2(int) CASCADE;

DROP FUNCTION IF EXISTS rcat_inout(int, int) CASCADE;

DROP FUNCTION IF EXISTS rcat_tab() CASCADE;

DROP PROCEDURE IF EXISTS rcat_proc(int) CASCADE;

DROP PROCEDURE IF EXISTS rcat_pio(int, int) CASCADE;

DROP FUNCTION IF EXISTS rcat_plain(int, text) CASCADE;

DROP FUNCTION IF EXISTS rcat_arr(int[]) CASCADE;

DROP FUNCTION IF EXISTS rcat_mod(numeric, varchar, interval) CASCADE;

DROP FUNCTION IF EXISTS rcat_var(int[]) CASCADE;

DROP FUNCTION IF EXISTS rcat_var2(int, text[]) CASCADE;

DROP FUNCTION IF EXISTS rcat_def(int, int, int) CASCADE;

DROP FUNCTION IF EXISTS rcat_pl(int) CASCADE;

DROP FUNCTION IF EXISTS rcat_attr(int) CASCADE;

