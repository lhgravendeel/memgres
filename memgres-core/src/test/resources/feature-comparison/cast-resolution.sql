-- ============================================================================
-- Feature Comparison: whether a cast exists, and what type it produces
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A cast was "render the source to text and feed the text to the target's
-- input function", so whether x::T was allowed depended on whether the text
-- happened to parse rather than on whether PostgreSQL has the conversion.
-- true::int8 answered 1 and '(1,1)'::point::money answered a price; where the
-- text did not parse it reported 22P02 about the input rather than the 42846
-- PostgreSQL raises about the types.
--
-- Beside it: float(p) names two different types depending on p — real up to 24
-- bits of mantissa and double precision above it — and a cast over a cast is
-- named after the type it ends at, not the one it passed through.
-- ============================================================================

SET search_path = public;


-- ============================================================================
-- A conversion PostgreSQL has no path for
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type boolean to bigint
-- end-expected-error
SELECT true::int8;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type integer to json
-- end-expected-error
SELECT 1::int::json;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type point to money
-- end-expected-error
SELECT '(1,1)'::point::money;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type date to interval
-- end-expected-error
SELECT '2020-01-01'::date::interval;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type integer to date
-- end-expected-error
SELECT 1::int::date;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type date to integer
-- end-expected-error
SELECT DATE '2020-01-01'::int;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type interval to integer
-- end-expected-error
SELECT interval '1 day'::int;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type boolean to date
-- end-expected-error
SELECT true::date;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type integer to point
-- end-expected-error
SELECT 1::point;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type money to integer
-- end-expected-error
SELECT '1234'::money::int;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type money to double precision
-- end-expected-error
SELECT '1234'::money::float8;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type tsvector to tsquery
-- end-expected-error
SELECT 'a'::tsvector::tsquery;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: ERROR: cannot cast type tsquery to tsvector
-- end-expected-error
SELECT 'a'::tsquery::tsvector;


-- ============================================================================
-- And every one it does have
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT 1::text AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT '1'::int AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT 1::int8 AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT true::int AS r;

-- begin-expected
-- columns: r
-- row: 1.5
-- end-expected
SELECT 1.5::numeric AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT 1::numeric::int AS r;

-- begin-expected
-- columns: r
-- row: 2020-01-01 00:00:00
-- end-expected
SELECT '2020-01-01'::date::timestamp AS r;

-- begin-expected
-- columns: r
-- row: {"a": 1}
-- end-expected
SELECT '{"a":1}'::json::jsonb AS r;

-- begin-expected
-- columns: r
-- row: 1 day
-- end-expected
SELECT '1 day'::interval::text AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT now()::date IS NOT NULL AS r;


-- An untyped literal has no source type to judge, so it casts to what it parses as.
-- begin-expected
-- columns: r
-- row: (1,1)
-- end-expected
SELECT '(1,1)'::point::text AS r;

-- begin-expected
-- columns: r
-- row: 2020-01-01
-- end-expected
SELECT '2020-01-01'::date::text AS r;


-- ============================================================================
-- float(p) is two types, and its precision has bounds
-- ============================================================================

-- begin-expected
-- columns: r
-- row: real
-- end-expected
SELECT pg_typeof(1::float(24))::text AS r;

-- begin-expected
-- columns: r
-- row: double precision
-- end-expected
SELECT pg_typeof(1::float(25))::text AS r;

-- begin-expected
-- columns: r
-- row: real
-- end-expected
SELECT pg_typeof(1::float(1))::text AS r;

-- begin-expected
-- columns: r
-- row: double precision
-- end-expected
SELECT pg_typeof(1::float(53))::text AS r;

-- begin-expected
-- columns: r
-- row: double precision
-- end-expected
SELECT pg_typeof(1::float)::text AS r;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: precision for type float must be at least 1 bit
-- end-expected-error
SELECT 1::float(0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: precision for type float must be less than 54 bits
-- end-expected-error
SELECT 1::float(54);


-- real reads every spelling of infinity and NaN that double precision reads.
-- begin-expected
-- columns: r
-- row: Infinity
-- end-expected
SELECT 'inf'::float(24) AS r;

-- begin-expected
-- columns: r
-- row: Infinity
-- end-expected
SELECT 'Infinity'::float(24) AS r;

-- begin-expected
-- columns: r
-- row: -Infinity
-- end-expected
SELECT '-inf'::float(24) AS r;

-- begin-expected
-- columns: r
-- row: NaN
-- end-expected
SELECT 'nan'::float(24) AS r;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type real: "zzz"
-- end-expected-error
SELECT 'zzz'::float(24);


-- ============================================================================
-- What a cast column is called
-- ============================================================================

-- begin-expected
-- columns: int4
-- row: 1
-- end-expected
SELECT 1::int;

-- begin-expected
-- columns: oid
-- row: 1
-- end-expected
SELECT 1::int::oid;

-- begin-expected
-- columns: jsonb
-- row: {"a": 1}
-- end-expected
SELECT '{"a":1}'::json::jsonb;

-- begin-expected
-- columns: name
-- row: x
-- end-expected
SELECT 'x'::varchar::name;

-- begin-expected
-- columns: timestamp
-- row: 2020-01-01 00:00:00
-- end-expected
SELECT '2020-01-01'::date::timestamp;

-- begin-expected
-- columns: float4
-- row: Infinity
-- end-expected
SELECT 'inf'::float(24);

-- begin-expected
-- columns: float8
-- row: Infinity
-- end-expected
SELECT 'inf'::float(25);

