-- ============================================================================
-- Feature Comparison: values taken from SQL text, read with a guard
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Each of these reached the client as XX000 "Internal error" carrying a Java
-- exception message, which tells an application the database is broken rather
-- than that its SQL was wrong. The worst did more: a $N number too large to be
-- one threw out of the extended-protocol Describe, and the escape left the
-- connection permanently one response ahead of its client — every later
-- statement handed back the previous statement's result set, silently.
-- ============================================================================

SET search_path = public;


DROP FUNCTION IF EXISTS zz_pgd_fn();

CREATE FUNCTION zz_pgd_fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;


-- ============================================================================
-- An exponent marker needs its digits
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: trailing junk after numeric literal at or near "1e"
-- end-expected-error
SELECT 1e;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: trailing junk after numeric literal at or near "1.5e"
-- end-expected-error
SELECT 1.5e;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: trailing junk after numeric literal at or near "1E+"
-- end-expected-error
SELECT 1E+;

-- begin-expected
-- columns: r
-- row: 100000
-- end-expected
SELECT 1e5 AS r;

-- begin-expected
-- columns: r
-- row: 0.0015
-- end-expected
SELECT 1.5e-3 AS r;

-- begin-expected
-- columns: r
-- row: 1000
-- end-expected
SELECT 1E+3 AS r;


-- ============================================================================
-- The special forms are grammar, so the wrong argument count is a syntax error
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ")"
-- end-expected-error
SELECT NULLIF(1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ")"
-- end-expected-error
SELECT COALESCE();

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT NULLIF(1,2) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT NULLIF(1,1) AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT COALESCE(NULL,2) AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT GREATEST(1,2,3) AS r;


-- ============================================================================
-- A SIMILAR TO pattern the engine cannot compile
-- ============================================================================

-- begin-expected-error
-- sqlstate: 2201B
-- message-like: ERROR: invalid regular expression: parentheses () not balanced
-- end-expected-error
SELECT 'abc' SIMILAR TO 'a(';

-- begin-expected-error
-- sqlstate: 2201B
-- message-like: ERROR: invalid regular expression: quantifier operand invalid
-- end-expected-error
SELECT 'abc' SIMILAR TO '*abc';

-- begin-expected-error
-- sqlstate: 2201B
-- message-like: ERROR: invalid regular expression: parentheses () not balanced
-- end-expected-error
SELECT 'abc' SIMILAR TO 'a)';

-- A brace that begins no quantifier is the character it is.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT 'a{b}' SIMILAR TO 'a{b}' AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT 'aaa' SIMILAR TO 'a{2,3}' AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT 'abc' SIMILAR TO '(a|b)%' AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT 'abc' SIMILAR TO 'a_c' AS r;


-- ============================================================================
-- A Julian day out of range is a date error
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22008
-- message-like: ERROR: date/time field value out of range: "J999999999999999999999999"
-- end-expected-error
SELECT 'J999999999999999999999999'::date;

-- begin-expected
-- columns: r
-- row: 2000-01-01
-- end-expected
SELECT 'J2451545'::date AS r;


-- ============================================================================
-- COST and ROWS take a positive number
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COST must be positive
-- end-expected-error
ALTER FUNCTION zz_pgd_fn() COST -1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "abc"
-- end-expected-error
ALTER FUNCTION zz_pgd_fn() COST abc;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: ROWS must be positive
-- end-expected-error
ALTER FUNCTION zz_pgd_fn() ROWS -5;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COST must be positive
-- end-expected-error
ALTER FUNCTION zz_pgd_fn() COST 0;


-- Writing an expression to a generated column is refused with 428C9, but only
-- once nothing earlier in the UPDATE path has refused it: with a FOR ALL TABLES
-- publication in place memgres reaches its replica-identity check first, which
-- is an ordering question for the DML path rather than for this file. The case
-- is asserted on a clean instance by ParseGuardTest instead.

-- ============================================================================
-- generate_series is strict
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::int AS r FROM generate_series(NULL::numeric, 10::numeric, 1);

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::int AS r FROM generate_series(1, NULL::int, 1);

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::int AS r FROM generate_series(1, 10, NULL::int);

-- begin-expected
-- columns: r
-- row: 10
-- end-expected
SELECT count(*)::int AS r FROM generate_series(1::numeric, 10::numeric, 1);

-- begin-expected
-- columns: r
-- row: 10
-- end-expected
SELECT count(*)::int AS r FROM generate_series(1, 10);


DROP FUNCTION IF EXISTS zz_pgd_fn();

