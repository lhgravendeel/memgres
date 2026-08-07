-- ============================================================================
-- Feature Comparison: the rest of what deciding a call needs
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Arity was read as an exact count, so a signature whose tail carries defaults
-- was matched only by a call passing every one of them, and a call passing
-- fewer went unjudged entirely.
--
-- An argument's type was read from a literal, a cast or a base table's column
-- and from nothing else, so an array constructor, a scalar subquery and a
-- column a subquery or a CTE produced all said nothing about themselves.
--
-- A name that no argument could choose between was reported only where some
-- argument had been written without a type.
--
-- A routine memgres answers without reading its arguments answered anyway when
-- the argument was NULL, though every one of them is strict: 88 of them, read
-- off the reference server one call at a time.
--
-- And a series over a bound with a fraction truncated it to a bigint, so the
-- same call answered whole numbers in the select list and fractions in FROM.
-- ============================================================================

SET search_path = public;

DROP TABLE IF EXISTS crr_t;

CREATE TABLE crr_t (n numeric, i int, b bigint, s text);

INSERT INTO crr_t VALUES (2, 2, 2, 'ab');

-- ============================================================================
-- How many arguments a signature takes
-- ============================================================================

-- A signature whose tail carries defaults is still that signature's when a call
-- passes fewer, so a wrong type reaching it is still a wrong type.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_terminate_backend(numeric) does not exist
-- end-expected-error
SELECT pg_terminate_backend(1.5);

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT pg_terminate_backend(1)::text AS r;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT pg_terminate_backend(1, 100)::text AS r;

-- begin-expected
-- columns: r
-- row:   abc
-- end-expected
SELECT lpad('abc', 5) AS r;

-- begin-expected
-- columns: r
-- row: xxabc
-- end-expected
SELECT lpad('abc', 5, 'x') AS r;

-- begin-expected
-- columns: r
-- row: 1,2
-- end-expected
SELECT array_to_string(ARRAY[1,2], ',') AS r;

-- begin-expected
-- columns: r
-- row: 1,2
-- end-expected
SELECT array_to_string(ARRAY[1,2], ',', 'n') AS r;

-- begin-expected
-- columns: r
-- row: axc
-- end-expected
SELECT regexp_replace('abc', 'b', 'x') AS r;

-- begin-expected
-- columns: r
-- row: axc
-- end-expected
SELECT regexp_replace('abc', 'b', 'x', 'g') AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT round(1.5)::text AS r;

-- begin-expected
-- columns: r
-- row: bcdef
-- end-expected
SELECT substr('abcdef', 2) AS r;

-- ============================================================================
-- What the statement says an argument is
-- ============================================================================

-- An array constructor is of the type of its elements.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_fill(integer, numeric[]) does not exist
-- end-expected-error
SELECT array_fill(1, ARRAY[2.5]);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_fill(integer, numeric[], integer[]) does not exist
-- end-expected-error
SELECT array_fill(1, ARRAY[2.5], ARRAY[1]);

-- begin-expected
-- columns: r
-- row: {1,1}
-- end-expected
SELECT array_fill(1, ARRAY[2])::text AS r;

-- begin-expected
-- columns: r
-- row: {1,1}
-- end-expected
SELECT array_fill(1, ARRAY[2], ARRAY[1])::text AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT array_length(ARRAY[1,2], 1)::text AS r;

-- So does a column a subquery or a CTE produced, and a scalar subquery's column.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_advisory_lock(n) FROM (SELECT 2::numeric AS n) t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(numeric) does not exist
-- end-expected-error
WITH c AS (SELECT 2::numeric AS n) SELECT pg_advisory_lock(n) FROM c;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_advisory_lock((SELECT n FROM crr_t));

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock(i) AS r FROM (SELECT 2::int AS i) t;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_unlock_all() AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
WITH c AS (SELECT 2::int AS i) SELECT pg_advisory_lock(i) AS r FROM c;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_unlock_all() AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_lock((SELECT i FROM crr_t)) AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT pg_advisory_unlock_all() AS r;

-- begin-expected
-- columns: r
-- row: abcd
-- end-expected
SELECT left('abcde', (SELECT 4)) AS r;

-- A column the engine could not type is a text one, and so is every column that
-- really is text; that answer is left alone rather than read as a declaration.
-- begin-expected
-- columns: r
-- row: 14
-- end-expected
SELECT val AS r FROM (SELECT 3 + 4 + 7 AS val) sub WHERE val > 10;

-- ============================================================================
-- A name that cannot be chosen
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42725
-- message-like: function to_hex(smallint) is not unique
-- end-expected-error
SELECT to_hex(10::smallint);

-- begin-expected
-- columns: r
-- row: a
-- end-expected
SELECT to_hex(10::int) AS r;

-- begin-expected
-- columns: r
-- row: a
-- end-expected
SELECT to_hex(10::bigint) AS r;

-- A signature written over "whatever was passed" is not one of two answers to
-- choose between: array_agg is declared over both anyarray and anynonarray.
-- begin-expected
-- columns: r
-- row: {2}
-- end-expected
SELECT array_agg(i)::text AS r FROM crr_t;

-- begin-expected
-- columns: r
-- row: {ab}
-- end-expected
SELECT array_agg(s)::text AS r FROM crr_t;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT max(i)::text AS r FROM crr_t;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT greatest(1::int, 2::bigint)::text AS r;

-- ============================================================================
-- A NULL argument to a routine that would answer anyway
-- ============================================================================
-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_advisory_lock(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_advisory_unlock(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_try_advisory_lock(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_advisory_xact_lock(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_advisory_lock(NULL, NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_advisory_lock(1, NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_sleep(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_sleep_for(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT setseed(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_terminate_backend(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_get_userbyid(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_blocking_pids(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_column_size(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_table_is_visible(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT current_schemas(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT make_time(NULL, NULL, NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT random_normal(NULL, NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT txid_snapshot_xmin(NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_encoding_to_char(NULL) AS r;

-- The same routines still answer when they are given something.
-- begin-expected
-- columns: r
-- row: 01:02:03
-- end-expected
SELECT make_time(1, 2, 3)::text AS r;

-- begin-expected
-- columns: r
-- row: UTF8
-- end-expected
SELECT pg_encoding_to_char(6) AS r;

-- begin-expected
-- columns: r
-- row: {public}
-- end-expected
SELECT current_schemas(false)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT pg_table_is_visible('crr_t'::regclass)::text AS r;

-- A multirange holds ranges, and NULL is not one.
-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT int4multirange(NULL) AS r;

-- begin-expected-error
-- sqlstate: 22004
-- message-like: multirange values cannot contain null members
-- end-expected-error
SELECT int4multirange(NULL, int4range(1,5));

-- begin-expected-error
-- sqlstate: 22004
-- message-like: multirange values cannot contain null members
-- end-expected-error
SELECT nummultirange(numrange(1,5), NULL);

-- begin-expected
-- columns: r
-- row: {[1,5)}
-- end-expected
SELECT int4multirange(int4range(1,5))::text AS r;

-- begin-expected
-- columns: r
-- row: {}
-- end-expected
SELECT int4multirange()::text AS r;

-- ============================================================================
-- A series over a bound with a fraction
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 1.5
-- row: 2.5
-- row: 3.5
-- end-expected
SELECT generate_series(1.5, 3.5)::text AS r;

-- begin-expected
-- columns: r
-- row: numeric
-- row: numeric
-- row: numeric
-- end-expected
SELECT pg_typeof(generate_series(1.5, 3.5))::text AS r;

-- begin-expected
-- columns: r
-- row: 1.5,2.5,3.5
-- end-expected
SELECT string_agg(g::text, ',') AS r FROM generate_series(1.5, 3.5) g;

-- begin-expected
-- columns: r
-- row: 1.0,1.5,2.0
-- end-expected
SELECT string_agg(g::text, ',') AS r FROM generate_series(1.0, 2.0, 0.5) g;

-- begin-expected
-- columns: r
-- row: 1,2,3
-- end-expected
SELECT string_agg(g::text, ',') AS r FROM generate_series(1, 3) g;

-- begin-expected
-- columns: r
-- row: 3,2,1
-- end-expected
SELECT string_agg(g::text, ',') AS r FROM generate_series(3, 1, -1) g;

DROP TABLE IF EXISTS crr_t;

