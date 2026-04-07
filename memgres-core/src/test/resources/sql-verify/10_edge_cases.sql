-- ============================================================
-- 10: Edge Cases, NULL Handling, Unicode, and Misc
-- ============================================================

-- === SELECT without FROM ===
SELECT 1;
SELECT 1, 2, 3;
SELECT 'hello';
SELECT true;
SELECT NULL;
SELECT 1 + 2 * 3;
SELECT (1 + 2) * 3;
SELECT 1::text || '!' ;
SELECT CURRENT_DATE;
SELECT CURRENT_TIMESTAMP;
SELECT GEN_RANDOM_UUID();
SELECT ARRAY[1,2,3];
SELECT ROW(1, 'hello', true);
SELECT GREATEST(1, 5, 3);
SELECT LEAST(1, 5, 3);

-- === NULL handling ===
SELECT NULL = NULL;
SELECT NULL <> NULL;
SELECT NULL < 1;
SELECT NULL > 1;
SELECT NULL AND true;
SELECT NULL AND false;
SELECT NULL OR true;
SELECT NULL OR false;
SELECT NOT NULL;
SELECT NULL IS NULL;
SELECT NULL IS NOT NULL;
SELECT 1 IS NULL;
SELECT 1 IS NOT NULL;
SELECT COALESCE(NULL, NULL, NULL);
SELECT COALESCE(NULL, 42);
SELECT NULL IN (1, 2, 3);
SELECT 1 IN (1, NULL, 3);
SELECT NULL NOT IN (1, 2, 3);
SELECT NULL BETWEEN 1 AND 10;
SELECT NULL LIKE '%';
SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END;
SELECT CASE WHEN NULL THEN 'true' ELSE 'false' END;
SELECT CASE WHEN NULL IS NULL THEN 'null is null' END;

-- === Boolean edge cases ===
SELECT true AND true;
SELECT true AND false;
SELECT false OR false;
SELECT true OR false;
SELECT NOT true;
SELECT NOT false;
SELECT true IS TRUE;
SELECT false IS TRUE;
SELECT NULL IS TRUE;
SELECT true IS FALSE;
SELECT false IS FALSE;
SELECT NULL IS FALSE;
SELECT NULL IS UNKNOWN;
SELECT true IS NOT UNKNOWN;
SELECT true IS DISTINCT FROM false;
SELECT true IS NOT DISTINCT FROM true;
SELECT NULL IS DISTINCT FROM NULL;
SELECT NULL IS NOT DISTINCT FROM NULL;

-- === String edge cases ===
SELECT '';
SELECT LENGTH('');
SELECT '' = '';
SELECT '' IS NULL;
SELECT '' <> NULL;
SELECT COALESCE('', 'fallback');
SELECT 'hello''world';
SELECT E'tab\there';
SELECT E'newline\nhere';
SELECT E'backslash\\here';
SELECT 'unicode: ' || U&'\0041';

-- === Numeric edge cases ===
SELECT 0;
SELECT -0;
SELECT 0.0;
SELECT -0.0;
SELECT 0 = -0;
SELECT 0.0 = -0.0;
SELECT 'NaN'::double precision = 'NaN'::double precision;
SELECT 'NaN'::double precision <> 'NaN'::double precision;
SELECT 'Infinity'::double precision > 999999999;
SELECT '-Infinity'::double precision < -999999999;
SELECT 1.0/3.0 * 3.0;
SELECT 0.1 + 0.2;
SELECT 999999999999999999::bigint;
SELECT -999999999999999999::bigint;

-- === Date edge cases ===
SELECT DATE '2024-02-29';
SELECT DATE '2023-02-29';
SELECT DATE '2024-12-31' + 1;
SELECT DATE '2024-01-01' - DATE '2023-01-01';
SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 second';
SELECT TIMESTAMP '2024-01-01 00:00:00' - INTERVAL '1 second';
SELECT AGE(TIMESTAMP '2024-01-01', TIMESTAMP '2024-01-01');

-- === Large/many values ===
CREATE TABLE many_cols (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int);
INSERT INTO many_cols VALUES (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
SELECT * FROM many_cols;
SELECT c1+c2+c3+c4+c5+c6+c7+c8+c9+c10+c11+c12+c13+c14+c15+c16+c17+c18+c19+c20 AS total FROM many_cols;
DROP TABLE many_cols;

-- Many rows insert
CREATE TABLE many_rows (id serial PRIMARY KEY, val int);
INSERT INTO many_rows (val) SELECT generate_series(1, 100);
SELECT COUNT(*) FROM many_rows;
SELECT SUM(val) FROM many_rows;
SELECT AVG(val) FROM many_rows;
SELECT MIN(val), MAX(val) FROM many_rows;
DROP TABLE many_rows;

-- === Generate series ===
SELECT * FROM generate_series(1, 5);
SELECT * FROM generate_series(1, 10, 2);
SELECT * FROM generate_series(10, 1, -2);
SELECT * FROM generate_series('2024-01-01'::date, '2024-01-07'::date, '1 day'::interval);

-- === CASE expression variants ===
SELECT CASE WHEN 1=1 THEN 'a' END;
SELECT CASE WHEN 1=2 THEN 'a' END;
SELECT CASE WHEN 1=2 THEN 'a' WHEN 1=1 THEN 'b' END;
SELECT CASE WHEN 1=2 THEN 'a' ELSE 'default' END;
SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;
SELECT CASE 3 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;
SELECT CASE 3 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END;

-- === IN with various types ===
SELECT 1 IN (1, 2, 3);
SELECT 4 IN (1, 2, 3);
SELECT 'a' IN ('a', 'b', 'c');
SELECT 'x' IN ('a', 'b', 'c');
SELECT NULL IN (1, 2, 3);
SELECT 1 IN (1, NULL, 3);
SELECT 1 NOT IN (2, 3, 4);
SELECT 1 NOT IN (1, 2, 3);
SELECT NULL NOT IN (1, 2, 3);

-- === BETWEEN ===
SELECT 5 BETWEEN 1 AND 10;
SELECT 0 BETWEEN 1 AND 10;
SELECT 10 BETWEEN 1 AND 10;
SELECT 'b' BETWEEN 'a' AND 'c';
SELECT DATE '2024-06-15' BETWEEN DATE '2024-01-01' AND DATE '2024-12-31';
SELECT 5 NOT BETWEEN 1 AND 10;
SELECT 0 NOT BETWEEN 1 AND 10;
SELECT 5 BETWEEN SYMMETRIC 10 AND 1;

-- === LIKE / pattern matching ===
SELECT 'hello' LIKE 'hel%';
SELECT 'hello' LIKE '%llo';
SELECT 'hello' LIKE 'h_llo';
SELECT 'hello' LIKE 'HELLO';
SELECT 'hello' ILIKE 'HELLO';
SELECT 'hello' NOT LIKE 'xyz%';
SELECT 'hello' SIMILAR TO '(hel|xyz)%';
SELECT 'hello' NOT SIMILAR TO '(abc|xyz)%';
SELECT 'hello' ~ '^h.*o$';
SELECT 'HELLO' ~* '^h.*o$';
SELECT 'hello' !~ '^x';
SELECT 'HELLO' !~* '^x';

-- === Type checking ===
SELECT PG_TYPEOF(42);
SELECT PG_TYPEOF(42.0);
SELECT PG_TYPEOF('hello');
SELECT PG_TYPEOF(true);
SELECT PG_TYPEOF(NULL);
SELECT PG_TYPEOF(ARRAY[1,2]);
SELECT PG_TYPEOF('2024-01-01'::date);
SELECT PG_TYPEOF('{"a":1}'::jsonb);

-- === System info ===
SELECT CURRENT_USER;
SELECT CURRENT_SCHEMA;
SELECT CURRENT_DATABASE();
SELECT CURRENT_SETTING('search_path');
SELECT HAS_TABLE_PRIVILEGE(CURRENT_USER, 'pg_class', 'SELECT');
SELECT PG_TABLE_IS_VISIBLE('pg_class'::regclass);

-- === Unicode ===
CREATE TABLE unicode_test (id serial PRIMARY KEY, val text);
INSERT INTO unicode_test (val) VALUES ('hello');
INSERT INTO unicode_test (val) VALUES ('café');
INSERT INTO unicode_test (val) VALUES ('日本語');
INSERT INTO unicode_test (val) VALUES ('emoji: 🎉');
INSERT INTO unicode_test (val) VALUES ('Ñoño');
SELECT * FROM unicode_test ORDER BY id;
SELECT LENGTH(val), val FROM unicode_test ORDER BY id;
SELECT val FROM unicode_test WHERE val LIKE '%café%';
SELECT UPPER('café');
SELECT LOWER('CAFÉ');
DROP TABLE unicode_test;

-- === Row value comparison ===
SELECT (1, 2) = (1, 2);
SELECT (1, 2) = (1, 3);
SELECT (1, 2) < (1, 3);
SELECT (1, 2) < (2, 1);
SELECT (1, 'a') = (1, 'a');
SELECT ROW(1, 'hello') = ROW(1, 'hello');
SELECT ROW(1, 'hello') = ROW(1, 'world');
