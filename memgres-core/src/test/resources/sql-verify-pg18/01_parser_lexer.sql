\echo '=== 01_parser_lexer.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

-- identifiers and quoting
CREATE TABLE "MixedCase"("select" int, normal int, "has space" text);
INSERT INTO "MixedCase"("select", normal, "has space") VALUES (1, 2, 'ok');
SELECT "select", normal, "has space" FROM "MixedCase";
SELECT mixedcase.normal FROM "MixedCase" AS mixedcase;
SELECT "MixedCase".normal FROM "MixedCase";

-- strings and comments
SELECT 'simple string' AS s;
SELECT E'line1\nline2' AS escaped;
SELECT $$dollar quoted$$ AS dq;
SELECT $tag$another dollar quoted string$tag$ AS dq2;
SELECT 'abc' /* mid expression comment */ || 'def' AS concatenated;
SELECT 1 + 2 * 3 AS precedence1, (1 + 2) * 3 AS precedence2;
SELECT NOT true AND false AS precedence3, NOT (true AND false) AS precedence4;

-- reserved words / aliases
SELECT 1 AS "from", 2 AS "where", 3 AS "group";
SELECT 1 AS x, 2 AS x;

-- invalid syntax variations
SELECT;
SELECT , 1;
SELECT 1 2;
SELECT (1 + 2));
SELECT ((1 + 2);
SELECT * FROM;
INSERT INTO VALUES (1);
INSERT test VALUES (1);
UPDATE SET x = 1;
DELETE test WHERE 1 = 1;
CREATE TABLE t_parser_01 (a int, b text;
CREATE TABLE t_parser_02 a int);
CREATE TABLE t_parser_03 ((a int));
CREATE TABLE t_parser_04 (a int,, b int);
CREATE TABLE t_parser_05 (a int b int);
ALTER TABLE t_missing ADD COLUMN;
DROP TABLE IF EXISTS;
CREATE VIEW v_bad AS;
CREATE INDEX ON "MixedCase";
CREATE INDEX idx_bad ON "MixedCase" ();
SELECT 1 FROM "MixedCase" WHERE ;
SELECT 1 FROM "MixedCase" ORDER ;
SELECT 1 FROM "MixedCase" GROUP BY ;
SELECT 1 FROM "MixedCase" HAVING ;
SELECT CASE WHEN true THEN 1 ELSE END;
SELECT CASE WHEN THEN 1 ELSE 2 END;
SELECT CAST(1 AS );
SELECT 1::;
SELECT ARRAY[1, 2, ];
SELECT ARRAY[,1,2];
SELECT ARRAY[1 2];
SELECT INTERVAL '1 day' '2 days';
SELECT DATE '2024-01-01' 'junk';
SELECT * FROM "MixedCase" m JOIN ON m.normal = 1;
WITH cte AS () SELECT 1;
WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 3 SELECT * FROM cte;
MERGE INTO "MixedCase" USING "MixedCase" ON WHEN MATCHED THEN UPDATE SET normal = 0;

-- valid but tricky
SELECT 'It''s fine' AS quoted;
SELECT "select" + normal AS sum_cols FROM "MixedCase";
SELECT 1::int4 + 2::int8 AS widened, pg_typeof(1::int4 + 2::int8) AS widened_type;

DROP SCHEMA compat CASCADE;
