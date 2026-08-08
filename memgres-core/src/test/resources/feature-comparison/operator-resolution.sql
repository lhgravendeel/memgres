-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: integer || integer
-- end-expected-error
SELECT 1 || 2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: integer || integer
-- end-expected-error
SELECT 1 || 2 + 3;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: date ~~ unknown
-- end-expected-error
SELECT '2020-01-01'::date LIKE '2020%';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: unknown ~~ integer
-- end-expected-error
SELECT 'abc' LIKE 5;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: money + integer
-- end-expected-error
SELECT '1.00'::money + 1;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: tsvector @@ text
-- end-expected-error
SELECT 'fox:1'::tsvector @@ 'fox'::text;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: integer[] = bigint[]
-- end-expected-error
SELECT '{1,2}'::int[] = '{1,2}'::bigint[];

-- begin-expected-error
-- sqlstate: 42725
-- message-like: ERROR: operator is not unique: unknown @> unknown
-- end-expected-error
SELECT '{a,b}' @> '{a}';

-- begin-expected-error
-- sqlstate: 42725
-- message-like: ERROR: operator is not unique: unknown + unknown
-- end-expected-error
SELECT NULL + NULL;

-- begin-expected-error
-- sqlstate: 42725
-- message-like: ERROR: operator is not unique: unknown - unknown
-- end-expected-error
SELECT NULL - NULL;

-- begin-expected-error
-- sqlstate: 42725
-- message-like: ERROR: operator is not unique: unknown * unknown
-- end-expected-error
SELECT 'a' * 'b';

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT 'abc' LIKE 'a%' AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT 'x' ~ 'x' AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT 'a' || 'b' AS r;

-- begin-expected
-- columns: r
-- row: 1x
-- end-expected
SELECT 1 || 'x' AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT NULL || NULL IS NULL AS r;

-- begin-expected
-- columns: r
-- row: {1,2,3}
-- end-expected
SELECT ARRAY[1,2] || 3 AS r;

-- begin-expected
-- columns: r
-- row: {3,1,2}
-- end-expected
SELECT 3 || ARRAY[1,2] AS r;

-- begin-expected
-- columns: r
-- row: {a,b}
-- end-expected
SELECT ARRAY['a']::text[] || 'b'::char(1) AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT 1 OPERATOR(pg_catalog.+) 2 AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT 'a' OPERATOR(pg_catalog.||) 'b' AS r;

