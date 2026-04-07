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

CREATE TABLE tgt(
  id int PRIMARY KEY,
  a int,
  b text DEFAULT 'd',
  c int GENERATED ALWAYS AS IDENTITY
);

CREATE TABLE src(
  id int,
  a int,
  b text
);

INSERT INTO tgt(id, a, b) VALUES
(1, 10, 'x'),
(2, 20, 'y'),
(3, 30, 'z');

INSERT INTO src VALUES
(1, 100, 'sx'),
(4, 400, 'sw'),
(4, 401, 'sw2'),
(5, NULL, NULL);

-- RETURNING depth
INSERT INTO tgt(id, a, b)
VALUES (10, 1000, 'r1')
RETURNING id, a, b, c, a * 2 AS doubled, pg_typeof(a), pg_typeof(b);

UPDATE tgt AS t
SET a = t.a + 1, b = upper(t.b)
WHERE t.id IN (1,2)
RETURNING t.id, t.a, t.b, t.c, t.a + 10 AS plus_ten;

DELETE FROM tgt t
WHERE t.id = 3
RETURNING t.id, t.a, t.b, t.c;

-- MERGE depth
MERGE INTO tgt AS t
USING src AS s
ON t.id = s.id
WHEN MATCHED AND s.a IS NOT NULL THEN
  UPDATE SET a = s.a, b = coalesce(s.b, t.b)
WHEN NOT MATCHED AND s.id IS NOT NULL THEN
  INSERT (id, a, b) VALUES (s.id, s.a, s.b);

SELECT * FROM tgt ORDER BY id;

CREATE TABLE src2(id int, a int, flag text);
INSERT INTO src2 VALUES
(1, 111, 'upd'),
(2, 222, 'del'),
(6, 666, 'ins');

MERGE INTO tgt AS t
USING src2 AS s
ON t.id = s.id
WHEN MATCHED AND s.flag = 'del' THEN
  DELETE
WHEN MATCHED AND s.flag = 'upd' THEN
  UPDATE SET a = s.a, b = s.flag
WHEN NOT MATCHED THEN
  INSERT (id, a, b) VALUES (s.id, s.a, s.flag);

SELECT * FROM tgt ORDER BY id;

-- scoping and alias traps
WITH tgt AS (
  SELECT id, a FROM compat.tgt WHERE id < 10
)
SELECT tgt.id, tgt.a FROM tgt ORDER BY id;

SELECT t.id,
       (SELECT max(s.a) FROM src s WHERE s.id = t.id) AS max_a
FROM tgt t
ORDER BY t.id;

SELECT t.id AS x, t.a
FROM tgt t
ORDER BY x;

SELECT sub.id, sub.a
FROM (
  SELECT t.id, t.a, row_number() OVER (ORDER BY t.id) AS rn
  FROM tgt t
) sub
WHERE sub.rn >= 1
ORDER BY sub.id;

-- bad RETURNING / MERGE / scoping cases
INSERT INTO tgt(id, a, b) VALUES (1, 1, 'dup') RETURNING nope;
UPDATE tgt SET a = a + 1 RETURNING missing_col;
DELETE FROM tgt RETURNING tgt.nope;
MERGE INTO tgt AS t
USING src AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET nope = 1;
MERGE INTO tgt AS t
USING src AS s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, a, b, c) VALUES (s.id, s.a, s.b, 1);
MERGE INTO tgt AS t
USING src AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET a = (SELECT id FROM src);
MERGE INTO tgt AS t
USING src AS s
ON t.id = s.id
WHEN MATCHED THEN DELETE
WHEN MATCHED THEN UPDATE SET a = 1;
SELECT id FROM tgt t JOIN src s ON id = s.id;
SELECT t.id AS x, t.a FROM tgt t ORDER BY missing_alias;
WITH tgt AS (SELECT 1 AS id) SELECT compat.tgt.id FROM tgt;
UPDATE tgt AS x SET a = x.a + 1 FROM src AS x WHERE x.id = x.id;
DELETE FROM tgt AS t USING src AS t WHERE t.id = t.id;

DROP SCHEMA compat CASCADE;
