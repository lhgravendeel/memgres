-- ============================================================================
-- Feature Comparison: what a PL/pgSQL variable holds, and what it may return
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A variable holds values of its declared type. PostgreSQL coerces on every
-- assignment, not only on the initialiser: an int takes 2 from 1.7, a boolean
-- takes true from 'yes', and neither takes 'abc' at all. Storing whatever the
-- expression produced let a variable hold something its own type could never
-- represent -- the boolean case answered false to 'yes', which is a wrong
-- answer rather than a missing error.
--
-- A row constructor being returned is not coerced either. RETURN ROW(x, x)
-- whose x is an integer does not fit a (bigint, bigint), and a bare 'a' is
-- unknown rather than text, so it fits no attribute at all.
--
-- A cursor opened without a name of its own is given a generated portal name,
-- and that is what the variable holds -- not the variable's own name, so two
-- functions each declaring a c open two different portals.
-- ============================================================================

SET search_path = public;

-- ============================================================================
-- An assignment is coerced to the declared type
-- ============================================================================

CREATE FUNCTION b9x_round() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x int; BEGIN x := 1.7; RETURN x::text; END $$;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT b9x_round() AS r;

CREATE FUNCTION b9x_parse() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x int; BEGIN x := '42'; RETURN x::text; END $$;

-- begin-expected
-- columns: r
-- row: 42
-- end-expected
SELECT b9x_parse() AS r;

CREATE FUNCTION b9x_totext() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x text; BEGIN x := 42; RETURN x; END $$;

-- begin-expected
-- columns: r
-- row: 42
-- end-expected
SELECT b9x_totext() AS r;

CREATE FUNCTION b9x_date() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x date; BEGIN x := '2020-01-01'; RETURN x::text; END $$;

-- begin-expected
-- columns: r
-- row: 2020-01-01
-- end-expected
SELECT b9x_date() AS r;

-- a boolean takes PostgreSQL's boolean input, where 'yes' is true
CREATE FUNCTION b9x_yes() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x boolean; BEGIN x := 'yes'; RETURN x::text; END $$;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT b9x_yes() AS r;

CREATE FUNCTION b9x_on() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x boolean; BEGIN x := 'on'; RETURN x::text; END $$;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT b9x_on() AS r;

CREATE FUNCTION b9x_off() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x boolean; BEGIN x := 'off'; RETURN x::text; END $$;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT b9x_off() AS r;

-- and a value the type cannot hold is refused rather than stored
CREATE FUNCTION b9x_bad() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x int; BEGIN x := 'abc'; RETURN x::text; END $$;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT b9x_bad();

CREATE FUNCTION b9x_badbool() RETURNS text LANGUAGE plpgsql AS $$ DECLARE x boolean; BEGIN x := 'abc'; RETURN x::text; END $$;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "abc"
-- end-expected-error
SELECT b9x_badbool();

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
DO $$ DECLARE x int; BEGIN SELECT 'abc' INTO x; END $$;

-- the declared width still bites
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
DO $$ DECLARE v varchar(3); BEGIN v := 'abcdef'; END $$;

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character(3)
-- end-expected-error
DO $$ DECLARE v char(3); BEGIN v := 'abcdef'; END $$;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
DO $$ DECLARE n numeric(3,1); BEGIN n := 12345.6; END $$;

DO $$ DECLARE v varchar(3); BEGIN v := 'abc'; END $$;

-- ============================================================================
-- A returned row is built of the return type's own attribute types
-- ============================================================================

CREATE TYPE b9x_two AS (q1 bigint, q2 bigint);

CREATE FUNCTION b9x_narrow(x int) RETURNS b9x_two LANGUAGE plpgsql AS $$ BEGIN RETURN ROW(x, x); END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT (b9x_narrow(42)).q1;

CREATE FUNCTION b9x_lits() RETURNS b9x_two LANGUAGE plpgsql AS $$ BEGIN RETURN ROW(1, 2); END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT (b9x_lits()).q1;

CREATE FUNCTION b9x_wide(x bigint) RETURNS b9x_two LANGUAGE plpgsql AS $$ BEGIN RETURN ROW(x, x, x); END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT (b9x_wide(42)).q1;

CREATE FUNCTION b9x_exact(x bigint) RETURNS b9x_two LANGUAGE plpgsql AS $$ BEGIN RETURN ROW(x, x); END $$;

-- begin-expected
-- columns: r
-- row: 42
-- end-expected
SELECT (b9x_exact(42)).q1 AS r;

CREATE FUNCTION b9x_cast(x int) RETURNS b9x_two LANGUAGE plpgsql AS $$ BEGIN RETURN ROW(x::bigint, x::bigint); END $$;

-- begin-expected
-- columns: r
-- row: 42
-- end-expected
SELECT (b9x_cast(42)).q1 AS r;

CREATE FUNCTION b9x_rowvar() RETURNS b9x_two LANGUAGE plpgsql AS $$ DECLARE r b9x_two; BEGIN r.q1 := 1; r.q2 := 2; RETURN r; END $$;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT (b9x_rowvar()).q1 AS r;

-- a bare literal is unknown rather than text, so it fits no attribute
CREATE TYPE b9x_t AS (a text);

CREATE FUNCTION b9x_unknown() RETURNS b9x_t LANGUAGE plpgsql AS $$ BEGIN RETURN ROW('a'); END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT (b9x_unknown()).a;

CREATE FUNCTION b9x_known() RETURNS b9x_t LANGUAGE plpgsql AS $$ BEGIN RETURN ROW('a'::text); END $$;

-- begin-expected
-- columns: r
-- row: a
-- end-expected
SELECT (b9x_known()).a AS r;

CREATE FUNCTION b9x_vc() RETURNS b9x_t LANGUAGE plpgsql AS $$ DECLARE v varchar(5) := 'a'; BEGIN RETURN ROW(v); END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT (b9x_vc()).a;

-- a trigger returns the row it wants written, and a scalar is not one
CREATE TABLE b9x_tr (a int, b text);

INSERT INTO b9x_tr VALUES (1, 'x');

CREATE FUNCTION b9x_badtrig() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$;

CREATE TRIGGER b9x_t1 BEFORE UPDATE ON b9x_tr FOR EACH ROW EXECUTE FUNCTION b9x_badtrig();

-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot return non-composite value from function returning composite type
-- end-expected-error
UPDATE b9x_tr SET a = a;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT a::text AS r FROM b9x_tr;

-- ============================================================================
-- The residuals around them
-- ============================================================================

-- ERRCODE takes a SQLSTATE or a condition name, and names no condition otherwise
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized exception condition "notvalid"
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = 'notvalid'; END $$;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: boom
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = 'division_by_zero'; END $$;

-- begin-expected-error
-- sqlstate: 12345
-- message-like: boom
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = '12345'; END $$;

-- begin-expected-error
-- sqlstate: ABCDE
-- message-like: boom
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = 'ABCDE'; END $$;

-- a word PostgreSQL did not recognise is quoted back the way it was written
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized RAISE statement option at or near "NoSuchOpt"
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'boom' USING NoSuchOpt = 'x'; END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized RAISE statement option at or near "nosuchopt"
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'boom' USING nosuchopt = 'x'; END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized GET DIAGNOSTICS item at or near "NoSuchItem"
-- end-expected-error
DO $$ DECLARE x text; BEGIN GET DIAGNOSTICS x = NoSuchItem; END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized GET DIAGNOSTICS item at or near "nosuchitem"
-- end-expected-error
DO $$ DECLARE x text; BEGIN GET DIAGNOSTICS x = nosuchitem; END $$;

-- EXECUTE runs every statement in the string; an INTO takes the last one's row
CREATE TABLE b9x_e (a int);

DO $$ BEGIN EXECUTE 'INSERT INTO b9x_e VALUES (1); INSERT INTO b9x_e VALUES (2)'; END $$;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT count(*)::text AS r FROM b9x_e;

CREATE FUNCTION b9x_last() RETURNS int LANGUAGE plpgsql AS $$ DECLARE v int; BEGIN EXECUTE 'SELECT 7; SELECT 8; SELECT 9' INTO v; RETURN v; END $$;

-- begin-expected
-- columns: r
-- row: 9
-- end-expected
SELECT b9x_last()::text AS r;

CREATE FUNCTION b9x_semi() RETURNS text LANGUAGE plpgsql AS $$ DECLARE v text; BEGIN EXECUTE 'SELECT ''a;b''' INTO v; RETURN v; END $$;

-- begin-expected
-- columns: r
-- row: a;b
-- end-expected
SELECT b9x_semi() AS r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "garbage"
-- end-expected-error
DO $$ BEGIN EXECUTE 'SELECT 1; garbage'; END $$;

-- ============================================================================
-- Teardown
-- ============================================================================

DROP TABLE b9x_tr;

DROP FUNCTION b9x_badtrig();

DROP TABLE b9x_e;

DROP FUNCTION b9x_round();

DROP FUNCTION b9x_parse();

DROP FUNCTION b9x_totext();

DROP FUNCTION b9x_date();

DROP FUNCTION b9x_yes();

DROP FUNCTION b9x_on();

DROP FUNCTION b9x_off();

DROP FUNCTION b9x_bad();

DROP FUNCTION b9x_badbool();

DROP FUNCTION b9x_narrow(int);

DROP FUNCTION b9x_lits();

DROP FUNCTION b9x_wide(bigint);

DROP FUNCTION b9x_exact(bigint);

DROP FUNCTION b9x_cast(int);

DROP FUNCTION b9x_rowvar();

DROP FUNCTION b9x_unknown();

DROP FUNCTION b9x_known();

DROP FUNCTION b9x_vc();

DROP FUNCTION b9x_last();

DROP FUNCTION b9x_semi();

DROP TYPE b9x_two;

DROP TYPE b9x_t;

