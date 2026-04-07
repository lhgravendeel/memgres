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

-- identifier casing and quoting
CREATE TABLE "MixedCase" ("sp ace" int, normal int, "select" text);
INSERT INTO "MixedCase" ("sp ace", normal, "select") VALUES (1, 2, 'ok');
SELECT "sp ace", normal, "select" FROM "MixedCase";
SELECT mixedcase.normal FROM mixedcase;
SELECT "MixedCase".normal FROM "MixedCase";

-- string literals and escapes
SELECT 'simple';
SELECT 'a''b';
SELECT E'line1\nline2';
SELECT U&'\0441\043B\043E\043D' AS unicode_escaped;
SELECT $$dollar$$, $tag$body$tag$;

-- bit strings and numeric oddities
SELECT B'1010', X'1F';
SELECT 1 + 2 * 3, (1 + 2) * 3, -2^2, (-2)^2;

-- comments in odd places
SELECT /* inline */ 1 AS x;
SELECT 1 -- trailing comment
;
SELECT/**/2;

-- invalid syntax variants
SELECT ;
SELECT ,1;
SELECT 1 1;
SELECT (1;
SELECT 1);
SELECT FROM pg_class;
SELECT 1 FROM FROM pg_class;
SELECT SELECT 1;
INSERT INTO "MixedCase" VALUES (1,2,3,);
CREATE TABLE bad1(,a int);
CREATE TABLE bad2(a int,, b int);
CREATE TABLE bad3(a int b int);
CREATE TABLE bad4(a int DEFAULT);
CREATE TABLE bad5(a int NOT);
CREATE TABLE bad6(a int CONSTRAINT);
CREATE TABLE bad7(a int PRIMARY);
CREATE TABLE bad8(a int REFERENCES);
SELECT 'unterminated;
SELECT $$unterminated;
SELECT E'bad\q';
SELECT 1 + * 2;
SELECT (SELECT 1;
SELECT CASE WHEN TRUE THEN 1 ELSE END;
SELECT CASE WHEN THEN 1 END;
SELECT ARRAY[1,2,];
SELECT ROW(1,2,);
SELECT func(,1);
SELECT 1 AS;
SELECT * FROM "MixedCase" WHERE ;
SELECT * FROM "MixedCase" ORDER BY;
SELECT * FROM "MixedCase" GROUP BY;
SELECT * FROM "MixedCase" HAVING;
SELECT * FROM "MixedCase" LIMIT;
SELECT * FROM "MixedCase" OFFSET;
SELECT * FROM "MixedCase" UNION;
SELECT * FROM "MixedCase" INTERSECT;
SELECT * FROM "MixedCase" EXCEPT;
SELECT * FROM "MixedCase" JOIN;
SELECT * FROM "MixedCase" JOIN "MixedCase" ON;
SELECT * FROM "MixedCase" WHERE normal IN ();
SELECT * FROM "MixedCase" WHERE normal BETWEEN AND 2;
SELECT * FROM "MixedCase" WHERE normal IS;
SELECT * FROM "MixedCase" WHERE normal IS DISTINCT;
SELECT * FROM "MixedCase" WHERE EXISTS ();
SELECT * FROM "MixedCase" WHERE normal = ANY ();
SELECT CAST(1 AS);
SELECT CAST AS int;
SELECT COLLATE "C";
SELECT 1::;
SELECT schema.;
SELECT .table;
SELECT OPERATOR(pg_catalog.+)(1,2,3);
SELECT * FROM "MixedCase" m("a","b");
SELECT [1,2,3];

DROP SCHEMA compat CASCADE;



-- additional statement-family syntax edge cases
CREATE TABLE syn1(a int, b text);
INSERT syn1 VALUES (1, 'x');
INSERT INTO syn1 (a b) VALUES (1, 'x');
INSERT INTO syn1 ((a), b) VALUES (1, 'x');
INSERT INTO syn1 (a, b VALUES (1, 'x');
INSERT INTO syn1 VALUES ((1, 'x'));
INSERT INTO syn1 VALUES 1, 2;
INSERT INTO syn1 VALUES ();
UPDATE syn1 a = 1 WHERE a = 1;
UPDATE syn1 SET WHERE a = 1;
UPDATE syn1 SET a = WHERE a = 1;
UPDATE syn1 SET a = 1, WHERE a = 1;
UPDATE syn1 SET (a, b) = (1) WHERE a = 1;
DELETE syn1 WHERE a = 1;
DELETE FROM WHERE a = 1;
DELETE FROM syn1 USING WHERE a = 1;
MERGE syn1 USING syn1 s ON syn1.a = s.a WHEN MATCHED THEN UPDATE SET a = 1;
MERGE INTO syn1 USING ON true WHEN MATCHED THEN UPDATE SET a = 1;
MERGE INTO syn1 t USING syn1 s ON WHEN MATCHED THEN UPDATE SET a = 1;
WITH x AS (SELECT 1), SELECT 1;
WITH RECURSIVE AS (SELECT 1) SELECT 1;
WITH x(y,) AS (SELECT 1) SELECT * FROM x;
CREATE VIEW vv AS;
CREATE VIEW vv2 (a,) AS SELECT 1;
CREATE INDEX ON syn1(a);
CREATE INDEX idx1 syn1(a);
ALTER TABLE syn1 ALTER COLUMN a TYPE;
ALTER TABLE syn1 ADD CONSTRAINT CHECK (a > 0);
ALTER TABLE syn1 ADD CONSTRAINT c1 CHECK;
ALTER TABLE syn1 RENAME COLUMN TO b;
DROP TABLE IF EXISTS;
TRUNCATE TABLE;
COMMENT ON TABLE syn1 IS;

