\echo '=== 02_types_and_casts.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN posint AS int CHECK (VALUE > 0);
CREATE TYPE pair AS (x int, y text);

SELECT true AS v, pg_typeof(true) AS t;
SELECT 42::smallint AS v, pg_typeof(42::smallint) AS t;
SELECT 42::integer AS v, pg_typeof(42::integer) AS t;
SELECT 42::bigint AS v, pg_typeof(42::bigint) AS t;
SELECT 123.45::numeric(10,2) AS v, pg_typeof(123.45::numeric(10,2)) AS t;
SELECT 1.25::real AS v, pg_typeof(1.25::real) AS t;
SELECT 1.25::double precision AS v, pg_typeof(1.25::double precision) AS t;
SELECT 'abc'::text AS v, pg_typeof('abc'::text) AS t;
SELECT 'abc'::varchar(5) AS v, pg_typeof('abc'::varchar(5)) AS t;
SELECT 'x'::char(3) AS v, pg_typeof('x'::char(3)) AS t;
SELECT E'\\xDEADBEEF'::bytea AS v, pg_typeof(E'\\xDEADBEEF'::bytea) AS t;
SELECT '00000000-0000-0000-0000-000000000000'::uuid AS v, pg_typeof('00000000-0000-0000-0000-000000000000'::uuid) AS t;
SELECT DATE '2024-02-29' AS v, pg_typeof(DATE '2024-02-29') AS t;
SELECT TIME '23:59:59' AS v, pg_typeof(TIME '23:59:59') AS t;
SELECT TIMESTAMP '2024-02-29 12:34:56' AS v, pg_typeof(TIMESTAMP '2024-02-29 12:34:56') AS t;
SELECT TIMESTAMPTZ '2024-02-29 12:34:56+00' AS v, pg_typeof(TIMESTAMPTZ '2024-02-29 12:34:56+00') AS t;
SELECT INTERVAL '1 day 2 hours 3 minutes' AS v, pg_typeof(INTERVAL '1 day 2 hours 3 minutes') AS t;
SELECT '{"a":1}'::json AS v, pg_typeof('{"a":1}'::json) AS t;
SELECT '{"a":1}'::jsonb AS v, pg_typeof('{"a":1}'::jsonb) AS t;
SELECT 'happy'::mood AS v, pg_typeof('happy'::mood) AS t;
SELECT 5::posint AS v, pg_typeof(5::posint) AS t;
SELECT ROW(1, 'a')::pair AS v, pg_typeof(ROW(1, 'a')::pair) AS t;
SELECT ARRAY[1,2,3] AS v, pg_typeof(ARRAY[1,2,3]) AS t;
SELECT ARRAY[['a','b'],['c','d']] AS v, pg_typeof(ARRAY[['a','b'],['c','d']]) AS t;

-- coercions and type resolution
SELECT 1 + 2.5 AS v, pg_typeof(1 + 2.5) AS t;
SELECT 1 / 2 AS v, pg_typeof(1 / 2) AS t;
SELECT 1::numeric / 2 AS v, pg_typeof(1::numeric / 2) AS t;
SELECT '123'::int + 1 AS v, pg_typeof('123'::int + 1) AS t;
SELECT CAST('123.45' AS numeric(10,2)) AS v;
SELECT CAST(NULL AS text) AS v, pg_typeof(CAST(NULL AS text)) AS t;
SELECT CAST(jsonb 'null' AS int) AS v, pg_typeof(CAST(jsonb 'null' AS int)) AS t;

-- valid comparisons and ordering
SELECT 'sad'::mood < 'happy'::mood AS enum_lt;
SELECT ARRAY[1,2] < ARRAY[1,3] AS array_lt;

-- bad inputs and bad casts
SELECT 'x'::boolean;
SELECT 'abc'::integer;
SELECT '999999999999999999999999999'::bigint;
SELECT '12.345'::numeric(4,2);
SELECT 'too long'::varchar(3);
SELECT '2024-02-30'::date;
SELECT '25:00:00'::time;
SELECT 'not a uuid'::uuid;
SELECT 'not json'::json;
SELECT 'angry'::mood;
SELECT (-1)::posint;
SELECT ROW(1)::pair;
SELECT ARRAY[1,'x'];
SELECT ARRAY[[1,2],[3]];
SELECT CAST('abc' AS bytea);
SELECT CAST(42 AS uuid);
SELECT CAST(ARRAY[1,2] AS integer);
SELECT CAST(ROW(1,'a') AS integer);
SELECT CAST('infinity' AS integer);

-- table-driven type behavior
CREATE TABLE type_table(
    a smallint,
    b integer,
    c bigint,
    d numeric(10,2),
    e real,
    f double precision,
    g text,
    h varchar(4),
    i char(2),
    j uuid,
    k date,
    l timestamp,
    m timestamptz,
    n interval,
    o jsonb,
    p mood,
    q posint,
    r pair,
    s int[]
);

INSERT INTO type_table VALUES (
    1, 2, 3, 4.50, 1.25, 2.5,
    'txt', 'abcd', 'z',
    '11111111-1111-1111-1111-111111111111',
    DATE '2024-01-01',
    TIMESTAMP '2024-01-01 10:00:00',
    TIMESTAMPTZ '2024-01-01 10:00:00+00',
    INTERVAL '2 days',
    '{"x":1}',
    'ok',
    9,
    ROW(7, 'seven'),
    ARRAY[1,2,3]
);

SELECT a, pg_typeof(a), d, pg_typeof(d), o, pg_typeof(o), p, pg_typeof(p), r, pg_typeof(r), s, pg_typeof(s)
FROM type_table;

-- assignment errors
INSERT INTO type_table(a) VALUES ('x');
INSERT INTO type_table(h) VALUES ('abcde');
INSERT INTO type_table(q) VALUES (0);
INSERT INTO type_table(p) VALUES ('angry');
INSERT INTO type_table(j) VALUES ('bad-uuid');

DROP SCHEMA compat CASCADE;
