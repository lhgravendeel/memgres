-- ============================================================================
-- Feature Comparison: which concatenation a || means
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL declares eleven concatenations and no more. Two of them take a
-- text on one side and anything that is not an array on the other, which is
-- what makes 'x' || 42 a string and 42 || 1 nothing at all.
--
-- memgres resolved nothing. It read the two values, decided from their shapes
-- whether they looked like arrays or JSON, and ran them together as strings
-- otherwise. Over the 1600 pairs of operand types a spread of forty types can
-- make, it disagreed with PostgreSQL about 1193: it answered for 772 pairs
-- there is no operator for, refused 28 there is, chose one for itself where
-- PostgreSQL says it cannot choose, and printed the values it did concatenate
-- as Java writes them rather than as their own type does.
--
-- "char" is the single byte PostgreSQL keeps for its own catalogs, and it puts
-- it in the internal category rather than among the string types. Nothing in
-- that category holds a preferred type, so nothing settles the choice between
-- text || text and text || anynonarray -- which is why a "char" beside a
-- string is a call PostgreSQL will not choose. memgres had no such type at
-- all: ::"char" was read as the blank-padded char, so the pair never arose.
--
-- The money values are not compared. How one is written is the server's
-- lc_monetary, the same way a timestamp's offset is its TimeZone.
-- ============================================================================

SET search_path = public;

SET TimeZone = 'UTC';

-- ============================================================================
-- A pair with no operator between them
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer || integer
-- end-expected-error
SELECT 1 || 2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer || date
-- end-expected-error
SELECT 1 || '2020-01-01'::date;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: boolean || numeric
-- end-expected-error
SELECT true || 1.5;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: timestamp without time zone || interval
-- end-expected-error
SELECT '2020-01-01'::timestamp || '1 day'::interval;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: smallint || bit
-- end-expected-error
SELECT 1::smallint || B'101';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: uuid || inet
-- end-expected-error
SELECT '00000000-0000-0000-0000-000000000001'::uuid || '10.0.0.1'::inet;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text || integer[]
-- end-expected-error
SELECT 'a'::text || ARRAY[1,2];

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer || text[]
-- end-expected-error
SELECT 1 || ARRAY['a','b']::text[];

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: "char" || text[]
-- end-expected-error
SELECT 'a'::"char" || ARRAY['a','b'];

-- ============================================================================
-- A pair with more than one, which PostgreSQL will not choose between
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: text || "char"
-- end-expected-error
SELECT 'a'::text || 'a'::"char";

-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: "char" || text
-- end-expected-error
SELECT 'a'::"char" || 'a'::text;

-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: "char" || "char"
-- end-expected-error
SELECT 'a'::"char" || 'a'::"char";

-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: character varying || "char"
-- end-expected-error
SELECT 'a'::varchar || 'a'::"char";

-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: name || "char"
-- end-expected-error
SELECT 'a'::name || 'a'::"char";

-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: unknown || "char"
-- end-expected-error
SELECT 'a' || 'a'::"char";

-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: "char" || unknown
-- end-expected-error
SELECT 'a'::"char" || 'a';

-- Beside anything that is not a string there is one candidate, and it runs.
-- begin-expected
-- columns: r
-- row: a1
-- end-expected
SELECT ('a'::"char" || 1)::text AS r;

-- begin-expected
-- columns: r
-- row: a2020-01-01
-- end-expected
SELECT ('a'::"char" || '2020-01-01'::date)::text AS r;

-- begin-expected
-- columns: r
-- row: 1a
-- end-expected
SELECT (1 || 'a'::"char")::text AS r;

-- The type the catalogs are written in is a type of its own.
-- begin-expected
-- columns: r
-- row: "char"
-- end-expected
SELECT pg_typeof('a'::"char")::text AS r;

-- begin-expected
-- columns: r
-- row: a
-- end-expected
SELECT ('abc'::"char")::text AS r;

-- begin-expected
-- columns: r
-- row: A
-- end-expected
SELECT (65::"char")::text AS r;

-- begin-expected
-- columns: r
-- row: character
-- end-expected
SELECT pg_typeof('a'::char(3))::text AS r;

-- ============================================================================
-- What the other operand is read as
-- ============================================================================
-- begin-expected
-- columns: r
-- row: a2020-01-01 00:00:00
-- end-expected
SELECT ('a' || '2020-01-01'::timestamp) AS r;

-- begin-expected
-- columns: r
-- row: a2020-01-01 00:00:00+00
-- end-expected
SELECT ('a' || '2020-01-01'::timestamptz) AS r;

-- begin-expected
-- columns: r
-- row: a1
-- end-expected
SELECT ('a' || 1::real) AS r;

-- begin-expected
-- columns: r
-- row: a1
-- end-expected
SELECT ('a' || 1::float8) AS r;

-- begin-expected
-- columns: r
-- row: a1
-- end-expected
SELECT ('a' || 1::numeric) AS r;

-- begin-expected
-- columns: r
-- row: a10.0.0.1/32
-- end-expected
SELECT ('a' || '10.0.0.1'::inet) AS r;

-- begin-expected
-- columns: r
-- row: a10.0.0.0/8
-- end-expected
SELECT ('a' || '10.0.0.0/8'::cidr) AS r;

-- begin-expected
-- columns: r
-- row: a1 day
-- end-expected
SELECT ('a' || '1 day'::interval) AS r;

-- begin-expected
-- columns: r
-- row: a{}
-- end-expected
SELECT ('a' || '{}'::json) AS r;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT ('a' || '{}'::jsonb) AS r;

-- begin-expected
-- columns: r
-- row: a[1,5)
-- end-expected
SELECT ('a' || int4range(1,5)) AS r;

-- begin-expected
-- columns: r
-- row: a{[1,5)}
-- end-expected
SELECT ('a' || int4multirange(int4range(1,5))) AS r;

-- begin-expected
-- columns: r
-- row: 'a'
-- end-expected
SELECT ('a' || 'a'::tsvector) AS r;

-- begin-expected
-- columns: r
-- row: a<a/>
-- end-expected
SELECT ('a' || '<a/>'::xml) AS r;

-- begin-expected
-- columns: r
-- row: a(1,2)
-- end-expected
SELECT ('a' || point '(1,2)') AS r;

-- begin-expected
-- columns: r
-- row: a\x01
-- end-expected
SELECT ('a'::text || '\x01'::bytea) AS r;

-- Written with no type of its own the left operand is the bytea's input.
-- begin-expected
-- columns: r
-- row: \x6101
-- end-expected
SELECT ('a' || '\x01'::bytea)::text AS r;

-- A blank-padded string loses its padding when it is read as a text.
-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('a'::char(3) || 'b') AS r;

-- begin-expected
-- columns: r
-- row: ba
-- end-expected
SELECT ('b' || 'a'::char(3)) AS r;

-- begin-expected
-- columns: r
-- row: a1
-- end-expected
SELECT ('a'::char(3) || 1) AS r;

-- ============================================================================
-- The arrays
-- ============================================================================
-- begin-expected
-- columns: r
-- row: {1,2,3}
-- end-expected
SELECT (ARRAY[1,2] || 3)::text AS r;

-- begin-expected
-- columns: r
-- row: {3,1,2}
-- end-expected
SELECT (3 || ARRAY[1,2])::text AS r;

-- begin-expected
-- columns: r
-- row: {1,2,3}
-- end-expected
SELECT (ARRAY[1,2] || ARRAY[3])::text AS r;

-- begin-expected
-- columns: r
-- row: {a,a,b}
-- end-expected
SELECT ('a'::text || ARRAY['a','b'])::text AS r;

-- begin-expected
-- columns: r
-- row: {a,b,a}
-- end-expected
SELECT (ARRAY['a','b'] || 'a'::text)::text AS r;

-- An array takes its element type from the left operand.
-- begin-expected
-- columns: r
-- row: {a,b,a}
-- end-expected
SELECT (ARRAY['a','b'] || 'a'::char(3))::text AS r;

-- begin-expected
-- columns: r
-- row: {1,2}
-- end-expected
SELECT (ARRAY[1,2] || NULL)::text AS r;

-- begin-expected
-- columns: r
-- row: {1,2}
-- end-expected
SELECT (NULL || ARRAY[1,2])::text AS r;

-- begin-expected
-- columns: r
-- row: {{1,2},{3,4}}
-- end-expected
SELECT (ARRAY[[1,2]] || ARRAY[3,4])::text AS r;

-- ============================================================================
-- An untyped operand is read as the operator's own type
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 1011
-- end-expected
SELECT ('101'::varbit || '1')::text AS r;

-- begin-expected
-- columns: r
-- row: 'a' 'b'
-- end-expected
SELECT ('a'::tsvector || 'b')::text AS r;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT ('{}'::jsonb || 'a')::text;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: "a" is not a valid binary digit
-- end-expected-error
SELECT (B'101' || 'a')::text;

-- ============================================================================
-- The concatenations a statement is actually written with
-- ============================================================================
-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('a' || 'b') AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('a'::text || 'b'::text) AS r;

-- begin-expected
-- columns: r
-- row: a1
-- end-expected
SELECT ('a' || 1) AS r;

-- begin-expected
-- columns: r
-- row: 1a
-- end-expected
SELECT (1 || 'a') AS r;

-- begin-expected
-- columns: r
-- row: atrue
-- end-expected
SELECT ('a' || true) AS r;

-- begin-expected
-- columns: r
-- row: abc
-- end-expected
SELECT ('a' || 'b' || 'c') AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT ('a' || NULL) AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT (NULL || NULL) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('a'::text || 'b'::varchar) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('a'::name || 'b'::text) AS r;

-- begin-expected
-- columns: r
-- row: {"a": 1, "b": 2}
-- end-expected
SELECT ('{"a":1}'::jsonb || '{"b":2}'::jsonb)::text AS r;

-- begin-expected
-- columns: r
-- row: \x0102
-- end-expected
SELECT ('\x01'::bytea || '\x02'::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: 101101
-- end-expected
SELECT (B'101' || B'101')::text AS r;

-- begin-expected
-- columns: r
-- row: 101101
-- end-expected
SELECT ('101'::varbit || '101'::varbit)::text AS r;

-- begin-expected
-- columns: r
-- row: 'a' 'b'
-- end-expected
SELECT ('a'::tsvector || 'b'::tsvector)::text AS r;

-- begin-expected
-- columns: r
-- row: 'a' | 'b'
-- end-expected
SELECT ('a'::tsquery || 'b'::tsquery)::text AS r;

