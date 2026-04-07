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

CREATE TABLE dml(
  id int PRIMARY KEY,
  a int,
  b text DEFAULT 'd',
  c int GENERATED ALWAYS AS IDENTITY
);

INSERT INTO dml(id, a) VALUES (1, 10), (2, 20) RETURNING id, a, b, c;
INSERT INTO dml DEFAULT VALUES RETURNING *;
UPDATE dml SET a = a + 1 WHERE id IN (1,2) RETURNING id, a, pg_typeof(a);
DELETE FROM dml WHERE id = 2 RETURNING *;

MERGE INTO dml AS t
USING (VALUES (1, 100), (4, 400)) AS s(id, a)
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET a = s.a
WHEN NOT MATCHED THEN INSERT (id, a) VALUES (s.id, s.a);

SELECT * FROM dml ORDER BY id;

-- bad DML
INSERT INTO dml(id, a) VALUES (1, 999);
INSERT INTO dml(id, a, b, c) VALUES (5, 5, 'x', 7);
INSERT INTO dml(id) VALUES ('x');
INSERT INTO dml(id, a) VALUES (6);
INSERT INTO dml(nope) VALUES (1);
UPDATE dml SET nope = 1;
UPDATE dml SET a = (SELECT id FROM dml);
DELETE FROM no_such;
MERGE INTO dml t
USING (VALUES (1)) s(id)
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET nope = 1;
INSERT INTO dml(id, a) SELECT 7;
INSERT INTO dml VALUES (8, 8);
INSERT INTO dml VALUES (9, 9, 'x', DEFAULT, 'extra');

DROP SCHEMA compat CASCADE;

