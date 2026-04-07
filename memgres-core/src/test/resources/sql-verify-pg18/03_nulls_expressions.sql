\echo '=== 03_nulls_expressions.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

CREATE TABLE expr_t(a int, b int, c text, d boolean);
INSERT INTO expr_t VALUES
    (1, 2, 'abc', true),
    (NULL, 2, NULL, false),
    (3, NULL, 'xyz', NULL),
    (NULL, NULL, NULL, NULL);

SELECT NULL = NULL AS eq_null,
       NULL IS NULL AS is_null,
       NULL IS NOT NULL AS is_not_null,
       NULL IS DISTINCT FROM NULL AS is_distinct,
       1 IS DISTINCT FROM NULL AS one_distinct_null;

SELECT TRUE AND NULL AS t_and_n,
       FALSE AND NULL AS f_and_n,
       TRUE OR NULL AS t_or_n,
       FALSE OR NULL AS f_or_n,
       NOT NULL::boolean AS not_null_bool;

SELECT COALESCE(NULL, NULL, 5) AS coalesce_v, pg_typeof(COALESCE(NULL, NULL, 5)) AS coalesce_t;
SELECT NULLIF(1, 1) AS nullif_same, NULLIF(1, 2) AS nullif_diff;
SELECT GREATEST(1, 5, 3) AS g1, LEAST(1, 5, 3) AS l1;
SELECT GREATEST(NULL::int, 5, 3) AS g2, LEAST(NULL::int, 5, 3) AS l2;

SELECT a, b,
       a + b AS sum_ab,
       a - b AS diff_ab,
       a * b AS mul_ab,
       a / b AS div_ab,
       a % b AS mod_ab,
       a ^ b AS pow_ab
FROM expr_t ORDER BY a NULLS LAST, b NULLS LAST;

SELECT c,
       c || '_suffix' AS concat1,
       concat(c, '_x') AS concat2,
       length(c) AS len,
       upper(c) AS up,
       lower(c) AS low,
       substring(c FROM 1 FOR 2) AS sub,
       position('b' in c) AS pos_b,
       c LIKE 'a%' AS like_a,
       c SIMILAR TO '(abc|xyz)' AS similar1,
       c ~ '^[a-z]+$' AS re1
FROM expr_t ORDER BY c NULLS LAST;

SELECT CASE WHEN a IS NULL THEN 'null-a' ELSE 'not-null-a' END AS case1,
       CASE a WHEN 1 THEN 'one' WHEN 3 THEN 'three' ELSE 'other' END AS case2
FROM expr_t ORDER BY a NULLS LAST;

SELECT count(*) AS cnt_all,
       count(a) AS cnt_a,
       sum(a) AS sum_a,
       avg(a) AS avg_a,
       min(c) AS min_c,
       max(c) AS max_c,
       bool_and(d) AS all_d,
       bool_or(d) AS any_d
FROM expr_t;

SELECT 1 IN (1,2,NULL) AS in1,
       3 IN (1,2,NULL) AS in2,
       3 NOT IN (1,2,NULL) AS not_in1,
       NULL IN (1,2,NULL) AS in_null;

-- expression errors
SELECT 1 / 0;
SELECT 9223372036854775807::bigint + 1;
SELECT abs('x');
SELECT substring(1 FROM 1 FOR 1);
SELECT repeat('a', -1);
SELECT position(NULL IN 'abc');
SELECT 1 LIKE '1';
SELECT 'abc' ~ '(';
SELECT CASE WHEN true THEN 1 ELSE 'x' END;
SELECT GREATEST(1, 'x');
SELECT COALESCE(1, 'x');
SELECT NULLIF(1, 'x');
SELECT 1 = ROW(1,2);
SELECT ARRAY[1,2] || 3;
SELECT 1 + INTERVAL '1 day';
SELECT date_part('bogus', TIMESTAMP '2024-01-01 00:00:00');
SELECT make_date(2024, 2, 30);
SELECT overlay('abc' placing 'xyz' from 0);

DROP SCHEMA compat CASCADE;
