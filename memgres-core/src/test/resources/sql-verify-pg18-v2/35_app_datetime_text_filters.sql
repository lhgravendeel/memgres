\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE app_t(
  id int PRIMARY KEY,
  created_at timestamptz,
  local_ts timestamp,
  due_date date,
  note text,
  deleted boolean,
  active boolean
);

INSERT INTO app_t VALUES
(1, TIMESTAMPTZ '2024-01-01 10:00:00+00', TIMESTAMP '2024-01-01 10:00:00', DATE '2024-01-02', 'Alpha', false, true),
(2, TIMESTAMPTZ '2024-03-31 01:30:00+00', TIMESTAMP '2024-03-31 01:30:00', DATE '2024-04-01', 'a_b', NULL, false),
(3, TIMESTAMPTZ '2024-10-27 01:30:00+00', TIMESTAMP '2024-10-27 01:30:00', DATE '2024-10-28', NULL, true, NULL);

-- common app-shaped datetime and filter expressions
SELECT CURRENT_DATE, CURRENT_TIMESTAMP, LOCALTIMESTAMP;
SELECT date_trunc('day', created_at), date_trunc('hour', local_ts) FROM app_t ORDER BY id;
SELECT extract(year FROM created_at), extract(month FROM due_date) FROM app_t ORDER BY id;
SELECT created_at AT TIME ZONE 'UTC', created_at AT TIME ZONE 'Europe/Amsterdam' FROM app_t ORDER BY id;
SELECT local_ts AT TIME ZONE 'UTC' FROM app_t ORDER BY id;
SELECT * FROM app_t
WHERE created_at >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
ORDER BY created_at, id;

SELECT * FROM app_t
WHERE deleted IS NOT TRUE
ORDER BY id;

SELECT * FROM app_t
WHERE active = TRUE
ORDER BY id;

SELECT coalesce(active, FALSE), nullif(note, ''), coalesce(note, 'missing')
FROM app_t ORDER BY id;

SELECT note LIKE 'A%', note ILIKE 'a%', note LIKE 'a\_b' ESCAPE '\'
FROM app_t ORDER BY id;

SELECT note ~ '^[A-Za-z_]+$', regexp_replace(coalesce(note, ''), '[A-Z]', 'x', 'g')
FROM app_t ORDER BY id;

SELECT split_part('a,b,c', ',', 2), strpos('abcdef', 'cd'), trim(both 'x' from 'xxabxx');

SELECT * FROM app_t
ORDER BY created_at DESC NULLS LAST, id DESC
LIMIT 2 OFFSET 0;

SELECT * FROM app_t
ORDER BY id
FETCH FIRST 2 ROWS ONLY;

-- bad app-shaped cases
SELECT note LIKE 'a%' ESCAPE 'xx' FROM app_t;
SELECT note ~ '(' FROM app_t;
SELECT regexp_replace('abc', '(', 'x');
SELECT date_trunc('nonesuch', created_at) FROM app_t;
SELECT extract(nonesuch FROM created_at) FROM app_t;
SELECT split_part('a,b,c', ',', 0);
SELECT * FROM app_t ORDER BY created_at FETCH FIRST -1 ROWS ONLY;

DROP SCHEMA compat CASCADE;
