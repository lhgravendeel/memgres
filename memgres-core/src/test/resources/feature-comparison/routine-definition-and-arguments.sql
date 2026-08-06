-- ============================================================================
-- Feature Comparison: what a definition must settle, and what a type can read
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A CREATE FUNCTION carries a list of options, and PostgreSQL reads them left
-- to right in whatever order they were written. memgres read them as an AS
-- clause and a LANGUAGE clause in one of two fixed arrangements, so anything
-- written a third way was left unread after the statement and the routine was
-- created from the part that had been reached. Nothing checked that a language
-- had been named at all, that there was a body to run, or that an option was
-- given once -- so a definition PostgreSQL refuses outright was accepted, and
-- what it created was not what had been written.
--
-- Separately, a value a type cannot read was reported as an internal error
-- rather than as bad input, or was quietly taken as zero, or was reported
-- against whichever type happened to do the reading. A client is told which
-- of its values is wrong and for which type; none of those three answers tells
-- it that.
-- ============================================================================

SET search_path = public;

DROP FUNCTION IF EXISTS rdef_f(int) CASCADE;

DROP FUNCTION IF EXISTS rdef_f() CASCADE;

DROP FUNCTION IF EXISTS rdef_ok() CASCADE;

DROP PROCEDURE IF EXISTS rdef_p() CASCADE;

CREATE FUNCTION rdef_ok() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;

-- ============================================================================
-- A definition that names no language, and one with nothing to run
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no language specified
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no language specified
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int AS $$ SELECT p $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no language specified
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int IMMUTABLE;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no language specified
-- end-expected-error
CREATE PROCEDURE rdef_p();

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no language specified
-- end-expected-error
CREATE PROCEDURE rdef_p() AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no function body specified
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no function body specified
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: no function body specified
-- end-expected-error
CREATE PROCEDURE rdef_p() LANGUAGE sql;

-- The language is looked up before there is any point asking what it runs.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "nosuchlang" does not exist
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int LANGUAGE nosuchlang;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "nosuchlang" does not exist
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int LANGUAGE nosuchlang AS $$ SELECT p $$;

-- ============================================================================
-- An option given twice, or twice over in two words
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ AS $$ SELECT 2 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql STRICT STRICT AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql STRICT CALLED ON NULL INPUT AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql IMMUTABLE STABLE AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql COST 1 COST 2 AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql ROWS 5 ROWS 6 AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql SECURITY DEFINER SECURITY INVOKER AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql LEAKPROOF NOT LEAKPROOF AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql PARALLEL SAFE PARALLEL UNSAFE AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int WINDOW WINDOW LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int SUPPORT a SUPPORT b LANGUAGE sql AS $$ SELECT 1 $$;

-- ALTER reads the same list, so the same repeat is the same refusal.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
ALTER FUNCTION rdef_ok() IMMUTABLE STABLE;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
ALTER FUNCTION rdef_ok() STRICT STRICT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
ALTER FUNCTION rdef_ok() COST 5 COST 6;

-- ============================================================================
-- What only a function may be
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() STRICT LANGUAGE sql AS $$ SELECT 1 $$;

-- Refused for being a procedure rather than for the repeat: the check is per
-- option, and this one comes first.
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() STRICT STRICT LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() IMMUTABLE LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() LEAKPROOF LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() COST 5 LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() ROWS 5 LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() PARALLEL SAFE LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() WINDOW LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: invalid attribute in procedure definition
-- end-expected-error
CREATE PROCEDURE rdef_p() CALLED ON NULL INPUT LANGUAGE sql AS $$ SELECT 1 $$;

-- ============================================================================
-- How many bodies, and how many items in the one
-- ============================================================================

-- Only the C language reads a second AS item.
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: only one AS item needed for language "sql"
-- end-expected-error
CREATE FUNCTION rdef_f(p int) RETURNS int LANGUAGE sql AS 'SELECT p', 'extra';

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: duplicate function body specified
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ RETURN 1;

-- A SQL-standard body says by itself that the language is SQL.
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: inline SQL function body only valid for language SQL
-- end-expected-error
CREATE FUNCTION rdef_f() RETURNS int LANGUAGE plpgsql RETURN 1;

-- ============================================================================
-- The definitions that must still be accepted, in the orders they are written
-- ============================================================================

DROP FUNCTION IF EXISTS rdef_f() CASCADE;

CREATE FUNCTION rdef_f() RETURNS int RETURN 1;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT rdef_f() AS r;

DROP FUNCTION IF EXISTS rdef_f() CASCADE;

CREATE FUNCTION rdef_f() RETURNS int STRICT RETURN 1;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT proisstrict::text AS r FROM pg_proc WHERE proname = 'rdef_f';

DROP FUNCTION IF EXISTS rdef_f() CASCADE;

CREATE FUNCTION rdef_f() RETURNS int AS $$ SELECT 1 $$ IMMUTABLE LANGUAGE sql STRICT;

-- begin-expected
-- columns: r
-- row: itrue
-- end-expected
SELECT provolatile::text || proisstrict::text AS r FROM pg_proc WHERE proname = 'rdef_f';

-- begin-expected
-- columns: r
-- row: CREATE OR REPLACE FUNCTION public.rdef_f() /  RETURNS integer /  LANGUAGE sql /  IMMUTABLE STRICT / AS $function$ SELECT 1 $function$ / 
-- end-expected
SELECT replace(pg_get_functiondef(oid), chr(10), ' / ') AS r FROM pg_proc WHERE proname = 'rdef_f';

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT rdef_f() AS r;

DROP FUNCTION IF EXISTS rdef_f() CASCADE;

-- An option ahead of the language used to leave the rest of the statement unread.
CREATE FUNCTION rdef_f(p int) RETURNS int WINDOW LANGUAGE sql AS $$ SELECT p $$;

-- begin-expected
-- columns: r
-- row: SELECT p
-- end-expected
SELECT trim(prosrc) AS r FROM pg_proc WHERE proname = 'rdef_f';

-- begin-expected
-- columns: r
-- row: w
-- end-expected
SELECT prokind AS r FROM pg_proc WHERE proname = 'rdef_f';

-- begin-expected
-- columns: r
-- row: CREATE OR REPLACE FUNCTION public.rdef_f(p integer) /  RETURNS integer /  LANGUAGE sql /  WINDOW / AS $function$ SELECT p $function$ / 
-- end-expected
SELECT replace(pg_get_functiondef(oid), chr(10), ' / ') AS r FROM pg_proc WHERE proname = 'rdef_f';

DROP FUNCTION IF EXISTS rdef_f(int) CASCADE;

CREATE FUNCTION rdef_f() RETURNS int LANGUAGE sql SET work_mem = '1MB' SET work_mem = '2MB' AS $$ SELECT 1 $$;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT rdef_f() AS r;

CREATE PROCEDURE rdef_p() SECURITY DEFINER LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected
-- columns: r
-- row: ptrue
-- end-expected
SELECT prokind::text || prosecdef::text AS r FROM pg_proc WHERE proname = 'rdef_p';

-- begin-expected
-- columns: r
-- row: CREATE OR REPLACE PROCEDURE public.rdef_p() /  LANGUAGE sql /  SECURITY DEFINER / AS $procedure$ SELECT 1 $procedure$ / 
-- end-expected
SELECT replace(pg_get_functiondef(oid), chr(10), ' / ') AS r FROM pg_proc WHERE proname = 'rdef_p';

DROP PROCEDURE IF EXISTS rdef_p() CASCADE;

CREATE PROCEDURE rdef_p() SET work_mem = '1MB' LANGUAGE sql AS $$ SELECT 1 $$;

-- begin-expected
-- columns: r
-- row: work_mem=1MB
-- end-expected
SELECT array_to_string(proconfig, ',') AS r FROM pg_proc WHERE proname = 'rdef_p';

-- ============================================================================
-- A value the type cannot read
-- ============================================================================
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT pg_advisory_lock('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT pg_advisory_unlock('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT pg_try_advisory_lock('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT pg_advisory_lock_shared('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT pg_advisory_xact_lock('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT pg_try_advisory_xact_lock('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT pg_advisory_unlock_shared('x');

-- The two-key form takes an integer apiece, so that is the type named.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT pg_advisory_lock('x', 'y');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type double precision: "x"
-- end-expected-error
SELECT pg_sleep('x');

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "x"
-- end-expected-error
SELECT pg_sleep_for('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT pg_terminate_backend('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT pg_cancel_backend('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT pg_blocking_pids('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type oid: "x"
-- end-expected-error
SELECT pg_get_userbyid('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT substr('abc', 'x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT substr('abc', 'x', 'y');

-- substring has a (text, text) form, so there the second argument is a pattern.
-- begin-expected
-- columns: r
-- row: oob
-- end-expected
SELECT substring('foobar', 'o.b') AS r;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type double precision: "x"
-- end-expected-error
SELECT setseed('x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type oid: "x"
-- end-expected-error
SELECT 'x'::oid;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type xid: "x"
-- end-expected-error
SELECT 'x'::xid;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type smallint: "x"
-- end-expected-error
SELECT 'x'::smallint;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type real: "x"
-- end-expected-error
SELECT 'x'::real;

-- ============================================================================
-- The range an OID covers
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 4294967295
-- end-expected
SELECT '4294967295'::oid::text AS r;

-- begin-expected
-- columns: r
-- row: 2147483648
-- end-expected
SELECT '2147483648'::oid::text AS r;

-- begin-expected
-- columns: r
-- row: 4294967295
-- end-expected
SELECT '-1'::oid::text AS r;

-- begin-expected
-- columns: r
-- row: 16384
-- end-expected
SELECT '16384'::oid::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('16384'::oid = 16384)::text AS r;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value "4294967296" is out of range for type oid
-- end-expected-error
SELECT '4294967296'::oid::text AS r;

-- ============================================================================
-- The good paths, unchanged
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock(42) AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(42)::text AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock('42') AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_try_advisory_lock(1, 2)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(1, 2)::text AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_unlock_all() AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_sleep(0) AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_sleep_for(interval '0 seconds') AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT setseed(0.5) AS r;

-- begin-expected
-- columns: r
-- row: bcd
-- end-expected
SELECT substr('abcdef', 2, 3) AS r;

-- begin-expected
-- columns: r
-- row: bcdef
-- end-expected
SELECT substr('abcdef', 2) AS r;

-- begin-expected
-- columns: r
-- row: bcd
-- end-expected
SELECT substring('abcdef' from 2 for 3) AS r;

-- begin-expected
-- columns: r
-- row: oob
-- end-expected
SELECT substring('foobar' from '%#"o_b#"%' for '#') AS r;

-- begin-expected
-- columns: r
-- row: 42
-- end-expected
SELECT ' 42 '::bigint::text AS r;

-- begin-expected
-- columns: r
-- row: 5
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1, 5);

-- begin-expected
-- columns: r
-- row: 100
-- end-expected
SELECT '100'::xid::text AS r;

DROP FUNCTION IF EXISTS rdef_f(int) CASCADE;

DROP FUNCTION IF EXISTS rdef_f() CASCADE;

DROP FUNCTION IF EXISTS rdef_ok() CASCADE;

DROP PROCEDURE IF EXISTS rdef_p() CASCADE;

