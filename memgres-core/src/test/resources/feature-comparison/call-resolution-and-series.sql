-- ============================================================================
-- Feature Comparison: resolving a call, and reading a generated series
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A literal written without a type has no type until a signature gives it one,
-- and a name declared over several kinds of value gives it none: sum is
-- declared over numbers and over intervals, so sum('1') names neither. Reading
-- the literal as a number regardless answered a call PostgreSQL does not have.
--
-- The same rules settle the calls that do resolve -- the string kind first,
-- then the kind's preferred type -- so abs('1') is the float8 one and
-- length('abc') the text one, and nothing that used to work stops working.
--
-- A series is not stored: every row of it follows from the first and the step.
-- Building the whole list first meant a five-million-row series cost five
-- million values before the query looked at one, and a series longer than
-- memgres was willing to hold was refused outright.
-- ============================================================================

SET search_path = public;

SET TimeZone = 'UTC';

-- ============================================================================
-- A name that means more than one thing is not chosen by an untyped literal
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function sum(unknown) is not unique
-- end-expected-error
SELECT sum('1');

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function avg(unknown) is not unique
-- end-expected-error
SELECT avg('1');

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function to_char(unknown, unknown) is not unique
-- end-expected-error
SELECT to_char('1', 'YYYY');

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function age(unknown) is not unique
-- end-expected-error
SELECT age(NULL);

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function date_trunc(unknown, unknown) is not unique
-- end-expected-error
SELECT date_trunc(NULL, NULL);

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function generate_series(unknown, unknown) is not unique
-- end-expected-error
SELECT count(*) AS r FROM generate_series('1', '3');

-- ...and one that means only one thing still does
-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT min('1') AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT max('1') AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT abs('1')::text AS r;

-- begin-expected
-- columns: r
-- row: 1.00
-- end-expected
SELECT round('1', 2)::text AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT length('abc')::text AS r;

-- begin-expected
-- columns: r
-- row: X
-- end-expected
SELECT upper('x') AS r;

-- begin-expected
-- columns: r
-- row: 'hello'
-- end-expected
SELECT quote_literal('hello') AS r;

-- begin-expected
-- columns: r
-- row: 'x'
-- end-expected
SELECT quote_nullable('x') AS r;

-- begin-expected
-- columns: r
-- row: a1
-- end-expected
SELECT concat('a', '1') AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT greatest(1, '2')::text AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT nullif(1, '1')::text AS r;

-- begin-expected
-- columns: r
-- row: 'b'
-- end-expected
SELECT ts_delete('a b'::tsvector, 'a')::text AS r;

-- a type name written as a call is a cast, whatever else carries the name
-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT bool('t')::text AS r;

-- begin-expected
-- columns: r
-- row: \x78
-- end-expected
SELECT bytea('x')::text AS r;

-- begin-expected
-- columns: r
-- row: x
-- end-expected
SELECT bpchar('x')::text AS r;

-- ============================================================================
-- A kind of value no signature takes there
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function generate_series(text, text) does not exist
-- end-expected-error
SELECT count(*) AS r FROM generate_series('1'::text, '3'::text);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.overlaps(integer, integer, integer, integer) does not exist
-- end-expected-error
SELECT 1 AS r WHERE (1, 2) OVERLAPS (3, 4);

-- ============================================================================
-- Two moments make a length of time, and one moment and a length make another
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 1 day
-- end-expected
SELECT (TIMESTAMPTZ '2020-01-02 00:00:00+00' - TIMESTAMPTZ '2020-01-01 00:00:00+00')::text AS r;

-- begin-expected
-- columns: r
-- row: interval
-- end-expected
SELECT pg_typeof(TIMESTAMPTZ '2020-01-02 00:00:00+00' - TIMESTAMPTZ '2020-01-01 00:00:00+00')::text AS r;

-- begin-expected
-- columns: r
-- row: 2020-01-01 01:30:00+00
-- end-expected
SELECT (TIMESTAMPTZ '2020-01-01 00:00:00+00' + INTERVAL '90 minutes')::text AS r;

-- begin-expected
-- columns: r
-- row: 2020-01-02 00:00:00+00
-- end-expected
SELECT (TIMESTAMPTZ '2020-01-01 00:00:00+00' + '1 day')::text AS r;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type timestamp with time zone: "1"
-- end-expected-error
SELECT (TIMESTAMPTZ '2020-01-01 00:00:00+00' - '1')::text AS r;

-- begin-expected
-- columns: r
-- row: 365 days
-- end-expected
SELECT (TIMESTAMP '2020-01-01' - TIMESTAMP '2019-01-01')::text AS r;

-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: date + unknown
-- end-expected-error
SELECT (DATE '2020-01-01' + '1')::text AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT (DATE '2020-01-02' - DATE '2020-01-01')::text AS r;

-- begin-expected
-- columns: r
-- row: 01:00:00
-- end-expected
SELECT (TIME '10:00' - TIME '09:00')::text AS r;

-- ============================================================================
-- A series is read, not held
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 12000000
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1, 12000000);

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT g::text AS r FROM generate_series(1, 5000000) g LIMIT 1;

-- begin-expected
-- columns: r
-- row: 4999999
-- row: 5000000
-- end-expected
SELECT g::text AS r FROM generate_series(1, 5000000) g OFFSET 4999998;

-- begin-expected
-- columns: r
-- row: 3000000
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1::int8, 3000000);

-- begin-expected
-- columns: r
-- row: 9223372036854775805
-- row: 9223372036854775806
-- row: 9223372036854775807
-- end-expected
SELECT g::text AS r FROM generate_series(9223372036854775805::int8, 9223372036854775807) g;

-- begin-expected
-- columns: r
-- row: 5
-- row: 3
-- row: 1
-- end-expected
SELECT g::text AS r FROM generate_series(5, 1, -2) g;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1, 0);

-- begin-expected
-- columns: r
-- row: 1
-- row: 1.25
-- row: 1.50
-- row: 1.75
-- row: 2.00
-- end-expected
SELECT g::text AS r FROM generate_series(1::numeric, 2, 0.25) g;

-- begin-expected
-- columns: r
-- row: 2000000
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1::numeric, 2000000, 1);

-- begin-expected
-- columns: r
-- row: 3506329
-- end-expected
SELECT count(*)::text AS r FROM generate_series('2000-01-01'::timestamp, '2400-01-01', '1 hour');

-- begin-expected
-- columns: r
-- row: 2020-01-31 00:00:00
-- row: 2020-02-29 00:00:00
-- row: 2020-03-29 00:00:00
-- row: 2020-04-29 00:00:00
-- end-expected
SELECT g::text AS r FROM generate_series('2020-01-31'::timestamp, '2020-05-01', '1 month') g;

-- begin-expected
-- columns: r
-- row: 2020-01-01 00:00:00
-- row: 2020-01-01 07:00:00
-- row: 2020-01-01 14:00:00
-- row: 2020-01-01 21:00:00
-- end-expected
SELECT g::text AS r FROM generate_series('2020-01-01'::timestamp, '2020-01-02', '7 hours') g;

-- begin-expected
-- columns: r
-- row: 36526
-- end-expected
SELECT count(*)::text AS r FROM generate_series('2000-01-01'::date, '2100-01-01', '1 day');

-- begin-expected
-- columns: r
-- row: 2020-01-01 00:00:00+00
-- row: 2020-01-01 02:00:00+00
-- row: 2020-01-01 04:00:00+00
-- row: 2020-01-01 06:00:00+00
-- end-expected
SELECT g::text AS r FROM generate_series('2020-01-01 00:00:00+00'::timestamptz, '2020-01-01 06:00:00+00', '2 hours') g;

-- begin-expected
-- columns: r
-- row: 6
-- row: 5
-- end-expected
SELECT g::text AS r FROM generate_series(1, 6) g ORDER BY g DESC LIMIT 2;

-- begin-expected
-- columns: r
-- row: 500500
-- end-expected
SELECT sum(g)::text AS r FROM generate_series(1, 1000) g;

-- begin-expected
-- columns: r
-- row: 12
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1, 4) a, generate_series(1, 3) b;

-- begin-expected
-- columns: r
-- row: 14
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1, 100) g WHERE g % 7 = 0;

-- begin-expected
-- columns: r | n
-- row: 10 | 1
-- row: 11 | 2
-- row: 12 | 3
-- end-expected
SELECT g::text AS r, o::text AS n FROM generate_series(10, 12) WITH ORDINALITY AS t(g, o);

-- begin-expected
-- columns: r
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT generate_series(1, 3)::text AS r;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM generate_series(1, 0, 1);

-- ============================================================================
-- What will not be built at all
-- ============================================================================
-- The size a request asks for is read before anything is allocated for it, so
-- a request too large is refused rather than taking the heap with it.

-- begin-expected-error
-- sqlstate: 54000
-- message-like: array size exceeds the maximum allowed (134217727)
-- end-expected-error
SELECT array_length(array_fill(1, ARRAY[400000000]), 1)::text AS r;

-- begin-expected-error
-- sqlstate: 54000
-- message-like: array size exceeds the maximum allowed (134217727)
-- end-expected-error
SELECT array_length(array_fill(1, ARRAY[20000, 20000]), 1)::text AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT array_length(array_fill(0, ARRAY[3]), 1)::text AS r;

-- begin-expected
-- columns: r
-- row: {{7,7},{7,7}}
-- end-expected
SELECT array_fill(7, ARRAY[2, 2])::text AS r;

-- begin-expected-error
-- sqlstate: 54000
-- message-like: requested length too large
-- end-expected-error
SELECT length(lpad('abc', 400000000, 'x'))::text AS r;

-- begin-expected-error
-- sqlstate: 54000
-- message-like: requested length too large
-- end-expected-error
SELECT length(repeat('ab', 1500000000))::text AS r;

-- begin-expected
-- columns: r
-- row: abc
-- end-expected
SELECT rpad('abc', 10, '') AS r;

