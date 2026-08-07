-- ============================================================================
-- Feature Comparison: whether a call's arguments reach a signature
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL resolves a call on the types its arguments were written with: an
-- argument reaches a parameter of its own type, or of one it casts to on its
-- own. memgres judged the argument's category instead, which is right about
-- kind and silent about type -- so a numeric reached a bigint parameter and
-- pg_advisory_lock(1.5) ran a function PostgreSQL says does not exist. There
-- is a cast from numeric to bigint, but only for an assignment, and a call is
-- not one.
--
-- What makes the stricter rule safe is that the conversions are PostgreSQL's
-- own list rather than an inference from the categories: nothing about a
-- category says a date reaches a timestamp, a cidr an inet, or a bit a bit
-- varying, and those calls have to keep working.
--
-- A sign is no part of a type either. Reading -4 as saying nothing left the
-- call it stood in to be resolved on its category's preferred type, so
-- abs(-4) came out a double precision where abs(4) was an integer.
-- ============================================================================

SET search_path = public;

-- ============================================================================
-- A numeric does not reach a bigint parameter
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_advisory_lock(1.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_unlock(numeric) does not exist
-- end-expected-error
SELECT pg_advisory_unlock(1.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_try_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_try_advisory_lock(1.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_xact_lock(numeric) does not exist
-- end-expected-error
SELECT pg_advisory_xact_lock(1.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(numeric, numeric) does not exist
-- end-expected-error
SELECT pg_advisory_lock(1.5, 2.5);

-- The value is beside the point: it is the type that cannot get there.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_advisory_lock(1::numeric);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(double precision) does not exist
-- end-expected-error
SELECT pg_advisory_lock(1.0::float8);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(real) does not exist
-- end-expected-error
SELECT pg_advisory_lock(1::real);

-- The same rule wherever an integer parameter is given something wider.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function to_hex(numeric) does not exist
-- end-expected-error
SELECT to_hex(10.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function round(numeric, numeric) does not exist
-- end-expected-error
SELECT round(1.5, 2.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function trunc(numeric, numeric) does not exist
-- end-expected-error
SELECT trunc(1.5, 2.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function width_bucket(numeric, numeric, numeric, numeric) does not exist
-- end-expected-error
SELECT width_bucket(1.0, 0.0, 10.0, 5.5);

-- ============================================================================
-- What an integer still reaches
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock(1) AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(1)::text AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock(1::smallint) AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(1)::text AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock(1::bigint) AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(1)::text AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock(1, 2) AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(1, 2)::text AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT setseed(0) AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT sqrt(4)::text AS r;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT ln(1)::text AS r;

-- begin-expected
-- columns: r
-- row: 8
-- end-expected
SELECT power(2, 3)::text AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT log(100)::text AS r;

-- begin-expected
-- columns: r
-- row: 1.00
-- end-expected
SELECT round(1, 2)::text AS r;

-- begin-expected
-- columns: r
-- row: 1.00
-- end-expected
SELECT trunc(1, 2)::text AS r;

-- begin-expected
-- columns: r
-- row:  1
-- end-expected
SELECT to_char(1, '9') AS r;

-- begin-expected
-- columns: r
-- row: a
-- end-expected
SELECT to_hex(10::bigint) AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT width_bucket(1.0, 0.0, 10.0, 5)::text AS r;

-- begin-expected
-- columns: r
-- row: {1,1}
-- end-expected
SELECT array_fill(1, ARRAY[2])::text AS r;

-- A numeric reaches the floating-point parameters, which PostgreSQL does cast to.
-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT setseed(0.5) AS r;

-- begin-expected
-- columns: r
-- row: 2.000000000000000
-- end-expected
SELECT sqrt(4.0)::text AS r;

-- An argument the statement says nothing about takes the parameter's own type.
-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT repeat('a', NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT chr(NULL) AS r;

-- begin-expected
-- columns: r
-- row: aa
-- end-expected
SELECT repeat('a', '2') AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock('42') AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(42)::text AS r;

-- ============================================================================
-- The conversions a category alone would not allow
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 1 year
-- end-expected
SELECT age('2020-01-01'::date, '2019-01-01'::date)::text AS r;

-- begin-expected
-- columns: r
-- row: 10.0.0.0
-- end-expected
SELECT host('10.0.0.0/8'::cidr) AS r;

-- begin-expected
-- columns: r
-- row: 10/8
-- end-expected
SELECT abbrev('10.0.0.0/8'::cidr) AS r;

-- begin-expected
-- columns: r
-- row: 8
-- end-expected
SELECT masklen('10.0.0.0/8'::cidr)::text AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT length('101'::bit(3))::text AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT length('101'::varbit)::text AS r;

-- begin-expected
-- columns: r
-- row: 08:00:2b:00:00:00
-- end-expected
SELECT trunc('08:00:2b:01:02:03'::macaddr)::text AS r;

-- begin-expected
-- columns: r
-- row: ABC
-- end-expected
SELECT upper('abc'::name) AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT length('abc'::varchar)::text AS r;

-- begin-expected
-- columns: r
-- row: abc
-- end-expected
SELECT btrim('abc'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: 97
-- end-expected
SELECT ascii('abc'::name)::text AS r;

-- begin-expected
-- columns: r
-- row: 1 mon
-- end-expected
SELECT justify_days('1 mon'::interval)::text AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT date_part('hour', '01:02:03'::time)::text AS r;

-- ============================================================================
-- A sign is no part of a type
-- ============================================================================
-- begin-expected
-- columns: r
-- row: abcd
-- end-expected
SELECT left('abcde', abs(-4)) AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(abs(-4))::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(-4)::text AS r;

-- begin-expected
-- columns: r
-- row: numeric
-- end-expected
SELECT pg_typeof(-4.5)::text AS r;

-- begin-expected
-- columns: r
-- row: 4
-- end-expected
SELECT abs(-4)::text AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock(-1) AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_advisory_unlock(-1)::text AS r;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_advisory_lock(-1.5);

-- ============================================================================
-- How a refusal names the routine
-- ============================================================================

-- The grammar-spelled forms are reported schema-qualified.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.substring(unknown, bigint, integer) does not exist
-- end-expected-error
SELECT substring('abcdef' from 2::bigint for 3);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.substring(unknown, bigint) does not exist
-- end-expected-error
SELECT substring('abcdef' from 2::bigint);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.overlay(unknown, unknown, bigint) does not exist
-- end-expected-error
SELECT overlay('abcdef' placing 'XY' from 2::bigint);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.ltrim(unknown, bigint) does not exist
-- end-expected-error
SELECT trim(leading 2::bigint from 'abc');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.rtrim(unknown, bigint) does not exist
-- end-expected-error
SELECT trim(trailing 2::bigint from 'abc');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.btrim(unknown, bigint) does not exist
-- end-expected-error
SELECT trim(both 2::bigint from 'abc');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.extract(unknown, bigint) does not exist
-- end-expected-error
SELECT extract(year from 2::bigint);

-- The same routines written as ordinary calls are named without the qualifier.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function substring(unknown, bigint, integer) does not exist
-- end-expected-error
SELECT substring('abcdef', 2::bigint, 3);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function substring(unknown, bigint) does not exist
-- end-expected-error
SELECT substring('abcdef', 2::bigint);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ltrim(unknown, bigint) does not exist
-- end-expected-error
SELECT ltrim('abc', 2::bigint);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function rtrim(unknown, bigint) does not exist
-- end-expected-error
SELECT rtrim('abc', 2::bigint);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function btrim(unknown, bigint) does not exist
-- end-expected-error
SELECT btrim('abc', 2::bigint);

-- And they still run when their arguments do reach.
-- begin-expected
-- columns: r
-- row: aXYef
-- end-expected
SELECT overlay('abcdef' placing 'XY' from 2 for 3) AS r;

-- begin-expected
-- columns: r
-- row: bcd
-- end-expected
SELECT substring('abcdef' from 2 for 3) AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT position('b' in 'abc')::text AS r;

-- begin-expected
-- columns: r
-- row: bc
-- end-expected
SELECT trim(leading 'a' from 'abc') AS r;

-- begin-expected
-- columns: r
-- row: 2020
-- end-expected
SELECT extract(year from '2020-01-01'::date)::text AS r;

SELECT pg_advisory_unlock_all();

