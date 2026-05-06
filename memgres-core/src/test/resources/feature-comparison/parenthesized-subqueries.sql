-- ============================================================================
-- Feature Comparison: Parenthesized Subqueries & Expressions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL's grammar allows extra parentheses in many positions. This file
-- exhaustively tests parenthesization at 1, 2, 3, 5, and 10 levels of nesting
-- across every SQL context where subqueries or expressions can appear.
--
-- The goal is to discover exactly where PG 18 accepts or rejects extra parens,
-- so Memgres can match that behavior precisely.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS paren_test CASCADE;
CREATE SCHEMA paren_test;
SET search_path = paren_test, public;

CREATE TABLE src (id serial PRIMARY KEY, val int, label text);
INSERT INTO src (val, label) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta'), (5, 'epsilon');

CREATE TABLE dst (id serial PRIMARY KEY, val int, label text);

CREATE TABLE empty_t (id int, val int);

-- ============================================================================
-- A. INSERT INTO ... (cols) (SELECT ...) — parenthesized SELECT source
-- ============================================================================

-- A1: single parens around SELECT
INSERT INTO dst (val, label) (SELECT val, label FROM src WHERE id = 1);

-- A2: double parens
INSERT INTO dst (val, label) ((SELECT val, label FROM src WHERE id = 2));

-- A3: triple parens
INSERT INTO dst (val, label) (((SELECT val, label FROM src WHERE id = 3)));

-- A4: 5 levels of parens
INSERT INTO dst (val, label) (((((SELECT val, label FROM src WHERE id = 4)))));

-- A5: 10 levels of parens
INSERT INTO dst (val, label) ((((((((((SELECT val, label FROM src WHERE id = 5))))))))));

-- A6: verify all 5 rows inserted
SELECT count(*) AS cnt FROM dst;

-- A7: no column list, single parens
TRUNCATE dst;
INSERT INTO dst (SELECT id, val, label FROM src WHERE id <= 2);

-- A8: no column list, double parens
INSERT INTO dst ((SELECT id, val, label FROM src WHERE id = 3));

-- A9: no column list, triple parens
INSERT INTO dst (((SELECT id, val, label FROM src WHERE id = 4)));

-- A10: no column list, 5 levels
INSERT INTO dst (((((SELECT id, val, label FROM src WHERE id = 5)))));

-- A11: verify
SELECT count(*) AS cnt FROM dst;

-- cleanup
TRUNCATE dst;

-- ============================================================================
-- B. INSERT INTO ... VALUES with parenthesized scalar subqueries
-- ============================================================================

-- B1: scalar subquery in VALUES, single parens
INSERT INTO dst (val, label) VALUES ((SELECT 42), 'b1');

-- B2: scalar subquery in VALUES, double parens
INSERT INTO dst (val, label) VALUES (((SELECT 43)), 'b2');

-- B3: scalar subquery in VALUES, triple parens
INSERT INTO dst (val, label) VALUES ((((SELECT 44))), 'b3');

-- B4: scalar subquery in VALUES, 5 levels
INSERT INTO dst (val, label) VALUES (((((SELECT 45)))), 'b4');

-- B5: scalar subquery in VALUES, 10 levels
INSERT INTO dst (val, label) VALUES ((((((((((SELECT 46))))))))), 'b5');

-- B6: multiple scalar subqueries with mixed paren depths
INSERT INTO dst (val, label) VALUES (((SELECT 47)), (((((SELECT 'b6'))))));

-- B7: verify
SELECT val, label FROM dst ORDER BY val;

-- cleanup
TRUNCATE dst;

-- ============================================================================
-- C. SELECT list — parenthesized scalar subqueries
-- ============================================================================

-- C1: single parens
SELECT (SELECT 1);

-- C2: double parens
SELECT ((SELECT 1));

-- C3: triple parens
SELECT (((SELECT 1)));

-- C4: 5 levels
SELECT (((((SELECT 1)))));

-- C5: 10 levels
SELECT ((((((((((SELECT 1))))))))));

-- C6: parenthesized scalar subquery with field access
CREATE TABLE comp_t (r record_type);
DROP TABLE IF EXISTS comp_t;
CREATE TYPE paren_rec AS (x int, y text);
CREATE TABLE comp_t (id int, r paren_rec);
INSERT INTO comp_t VALUES (1, ROW(10, 'hello'));

SELECT ((SELECT r FROM comp_t WHERE id = 1)).x;

-- C7: double parens with field access
SELECT (((SELECT r FROM comp_t WHERE id = 1))).x;

-- C8: parenthesized expression (not subquery)
SELECT (1 + 2);

-- C9: double parens on expression
SELECT ((1 + 2));

-- C10: 5 levels on expression
SELECT (((((1 + 2)))));

-- C11: 10 levels on expression
SELECT ((((((((((1 + 2))))))))));

-- C12: mixed — parenthesized subquery + parenthesized expression
SELECT ((SELECT 1)) + (((2 + 3)));

-- ============================================================================
-- D. WHERE clause — parenthesized conditions
-- ============================================================================

-- D1: parenthesized boolean expression
SELECT * FROM src WHERE (val > 2);

-- D2: double parens on boolean
SELECT * FROM src WHERE ((val > 2));

-- D3: 5 levels on boolean
SELECT * FROM src WHERE (((((val > 2)))));

-- D4: 10 levels on boolean
SELECT * FROM src WHERE ((((((((((val > 2))))))))));

-- D5: parenthesized subquery in WHERE
SELECT * FROM src WHERE val = (SELECT 3);

-- D6: double parens on subquery in WHERE
SELECT * FROM src WHERE val = ((SELECT 3));

-- D7: 5 levels on subquery in WHERE
SELECT * FROM src WHERE val = (((((SELECT 3)))));

-- ============================================================================
-- E. EXISTS — parenthesized subqueries
-- ============================================================================

-- E1: standard EXISTS
SELECT EXISTS (SELECT 1);

-- E2: EXISTS with double parens
SELECT EXISTS ((SELECT 1));

-- E3: EXISTS with triple parens
SELECT EXISTS (((SELECT 1)));

-- E4: EXISTS with 5 levels
SELECT EXISTS (((((SELECT 1)))));

-- E5: EXISTS with 10 levels
SELECT EXISTS ((((((((((SELECT 1))))))))));

-- E6: NOT EXISTS with double parens
SELECT NOT EXISTS ((SELECT 1 WHERE false));

-- E7: NOT EXISTS with 5 levels
SELECT NOT EXISTS (((((SELECT 1 WHERE false)))));

-- E8: EXISTS in WHERE clause, double parens
SELECT * FROM src WHERE EXISTS ((SELECT 1 FROM src s2 WHERE s2.id = src.id));

-- E9: EXISTS in WHERE clause, 5 levels
SELECT * FROM src WHERE EXISTS (((((SELECT 1 FROM src s2 WHERE s2.id = src.id)))));

-- ============================================================================
-- F. IN — parenthesized subqueries and value lists
-- ============================================================================

-- F1: IN with subquery, single parens (standard)
SELECT * FROM src WHERE val IN (SELECT val FROM src WHERE val <= 2);

-- F2: IN with subquery, double parens
SELECT * FROM src WHERE val IN ((SELECT val FROM src WHERE val <= 2));

-- F3: IN with subquery, triple parens
SELECT * FROM src WHERE val IN (((SELECT val FROM src WHERE val <= 2)));

-- F4: IN with subquery, 5 levels
SELECT * FROM src WHERE val IN (((((SELECT val FROM src WHERE val <= 2)))));

-- F5: IN with subquery, 10 levels
SELECT * FROM src WHERE val IN ((((((((((SELECT val FROM src WHERE val <= 2))))))))));

-- F6: NOT IN with double parens
SELECT * FROM src WHERE val NOT IN ((SELECT val FROM src WHERE val <= 2));

-- F7: IN with parenthesized value list (not subquery)
SELECT * FROM src WHERE val IN (1, 2, 3);

-- F8: IN with extra parens around individual values
SELECT * FROM src WHERE val IN ((1), (2), (3));

-- F9: IN with deep parens around individual values
SELECT * FROM src WHERE val IN (((1)), ((2)), ((3)));

-- ============================================================================
-- G. ANY / SOME / ALL — parenthesized subqueries
-- ============================================================================

-- G1: ANY with subquery, standard
SELECT * FROM src WHERE val = ANY (SELECT val FROM src WHERE val <= 2);

-- G2: ANY with subquery, double parens
SELECT * FROM src WHERE val = ANY ((SELECT val FROM src WHERE val <= 2));

-- G3: ANY with subquery, 5 levels
SELECT * FROM src WHERE val = ANY (((((SELECT val FROM src WHERE val <= 2)))));

-- G4: SOME with double parens (SOME = ANY synonym)
SELECT * FROM src WHERE val = SOME ((SELECT val FROM src WHERE val <= 2));

-- G5: ALL with subquery, standard
SELECT * FROM src WHERE val > ALL (SELECT val FROM src WHERE val <= 2);

-- G6: ALL with subquery, double parens
SELECT * FROM src WHERE val > ALL ((SELECT val FROM src WHERE val <= 2));

-- G7: ALL with subquery, 5 levels
SELECT * FROM src WHERE val > ALL (((((SELECT val FROM src WHERE val <= 2)))));

-- G8: ANY with array, standard
SELECT * FROM src WHERE val = ANY (ARRAY[1, 2, 3]);

-- G9: ANY with parenthesized array
SELECT * FROM src WHERE val = ANY ((ARRAY[1, 2, 3]));

-- ============================================================================
-- H. ARRAY constructor — parenthesized subqueries
-- ============================================================================

-- H1: ARRAY subquery, standard
SELECT ARRAY(SELECT val FROM src ORDER BY val);

-- H2: ARRAY subquery, double parens
SELECT ARRAY((SELECT val FROM src ORDER BY val));

-- H3: ARRAY subquery, triple parens
SELECT ARRAY(((SELECT val FROM src ORDER BY val)));

-- H4: ARRAY subquery, 5 levels
SELECT ARRAY(((((SELECT val FROM src ORDER BY val)))));

-- ============================================================================
-- I. FROM clause — parenthesized subqueries (should already work)
-- ============================================================================

-- I1: standard subquery in FROM
SELECT * FROM (SELECT val, label FROM src) sub;

-- I2: double parens in FROM
SELECT * FROM ((SELECT val, label FROM src)) sub;

-- I3: triple parens in FROM
SELECT * FROM (((SELECT val, label FROM src))) sub;

-- I4: 5 levels in FROM
SELECT * FROM (((((SELECT val, label FROM src))))) sub;

-- I5: 10 levels in FROM
SELECT * FROM ((((((((((SELECT val, label FROM src)))))))))) sub;

-- I6: nested subqueries in FROM, each with extra parens
SELECT * FROM ((SELECT * FROM ((SELECT val FROM src)) inner_sub)) outer_sub;

-- ============================================================================
-- J. CREATE VIEW AS — parenthesized SELECT
-- ============================================================================

-- J1: standard
CREATE VIEW paren_v1 AS SELECT val FROM src;

-- J2: single parens
CREATE VIEW paren_v2 AS (SELECT val FROM src);

-- J3: double parens
CREATE VIEW paren_v3 AS ((SELECT val FROM src));

-- J4: triple parens
CREATE VIEW paren_v4 AS (((SELECT val FROM src)));

-- J5: 5 levels
CREATE VIEW paren_v5 AS (((((SELECT val FROM src)))));

-- J6: verify a view works
SELECT * FROM paren_v5 ORDER BY val;

-- ============================================================================
-- K. CREATE TABLE AS — parenthesized SELECT
-- ============================================================================

-- K1: standard
CREATE TABLE ctas1 AS SELECT val FROM src;

-- K2: single parens
CREATE TABLE ctas2 AS (SELECT val FROM src);

-- K3: double parens
CREATE TABLE ctas3 AS ((SELECT val FROM src));

-- K4: 5 levels
CREATE TABLE ctas4 AS (((((SELECT val FROM src)))));

-- K5: verify
SELECT count(*) AS cnt FROM ctas4;

-- ============================================================================
-- L. CTE (WITH) — parenthesized CTE bodies
-- ============================================================================

-- L1: standard CTE
WITH cte AS (SELECT val FROM src) SELECT * FROM cte;

-- L2: double parens around CTE body
WITH cte AS ((SELECT val FROM src)) SELECT * FROM cte;

-- L3: triple parens
WITH cte AS (((SELECT val FROM src))) SELECT * FROM cte;

-- L4: 5 levels
WITH cte AS (((((SELECT val FROM src))))) SELECT * FROM cte;

-- L5: multiple CTEs with mixed paren depths
WITH cte1 AS ((SELECT val FROM src WHERE val <= 2)),
     cte2 AS (((SELECT val FROM src WHERE val > 2)))
SELECT * FROM cte1 UNION ALL SELECT * FROM cte2 ORDER BY val;

-- L6: CTE with column list + parenthesized body
WITH cte (v) AS ((SELECT val FROM src)) SELECT * FROM cte;

-- L7: CTE with column list + 5 levels
WITH cte (v) AS (((((SELECT val FROM src))))) SELECT * FROM cte;

-- L8: recursive CTE with double parens
WITH RECURSIVE cnt (n) AS (
    (SELECT 1)
    UNION ALL
    (SELECT n + 1 FROM cnt WHERE n < 5)
)
SELECT * FROM cnt;

-- L9: recursive CTE with triple parens on each arm
WITH RECURSIVE cnt (n) AS (
    ((SELECT 1))
    UNION ALL
    ((SELECT n + 1 FROM cnt WHERE n < 5))
)
SELECT * FROM cnt;

-- ============================================================================
-- M. Set operations — parenthesized arms
-- ============================================================================

-- M1: UNION with parenthesized arms
(SELECT val FROM src WHERE val <= 2) UNION (SELECT val FROM src WHERE val > 3);

-- M2: double parens on UNION arms
((SELECT val FROM src WHERE val <= 2)) UNION ((SELECT val FROM src WHERE val > 3));

-- M3: triple parens on UNION arms
(((SELECT val FROM src WHERE val <= 2))) UNION (((SELECT val FROM src WHERE val > 3)));

-- M4: 5 levels on UNION arms
(((((SELECT val FROM src WHERE val <= 2))))) UNION (((((SELECT val FROM src WHERE val > 3)))));

-- M5: UNION ALL with double parens
((SELECT val FROM src WHERE val <= 2)) UNION ALL ((SELECT val FROM src WHERE val > 3));

-- M6: INTERSECT with double parens
((SELECT val FROM src)) INTERSECT ((SELECT val FROM src WHERE val <= 3));

-- M7: EXCEPT with double parens
((SELECT val FROM src)) EXCEPT ((SELECT val FROM src WHERE val <= 3));

-- M8: chained set ops with mixed parens
((SELECT val FROM src WHERE val = 1))
UNION
((SELECT val FROM src WHERE val = 2))
UNION
((SELECT val FROM src WHERE val = 3))
ORDER BY val;

-- M9: set op with ORDER BY/LIMIT inside parens
(SELECT val FROM src ORDER BY val LIMIT 2) UNION ALL (SELECT val FROM src ORDER BY val DESC LIMIT 2);

-- ============================================================================
-- N. CASE expression — parenthesized values
-- ============================================================================

-- N1: parenthesized CASE result
SELECT CASE WHEN true THEN (1) ELSE (2) END;

-- N2: double parens on CASE result
SELECT CASE WHEN true THEN ((1)) ELSE ((2)) END;

-- N3: parenthesized subquery in CASE
SELECT CASE WHEN true THEN (SELECT 1) ELSE (SELECT 2) END;

-- N4: double parens on subquery in CASE
SELECT CASE WHEN true THEN ((SELECT 1)) ELSE ((SELECT 2)) END;

-- N5: 5 levels on subquery in CASE
SELECT CASE WHEN true THEN (((((SELECT 1))))) ELSE (((((SELECT 2))))) END;

-- N6: parenthesized CASE condition
SELECT CASE WHEN ((true)) THEN 1 ELSE 2 END;

-- N7: parenthesized simple CASE operand
SELECT CASE (1) WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;

-- N8: double parens on simple CASE operand
SELECT CASE ((1)) WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;

-- ============================================================================
-- O. Function arguments — parenthesized expressions and subqueries
-- ============================================================================

-- O1: parenthesized arg
SELECT abs((- 5));

-- O2: double parens on arg
SELECT abs(((- 5)));

-- O3: 5 levels on arg
SELECT abs(((((- 5)))));

-- O4: scalar subquery as function arg
SELECT abs((SELECT -5));

-- O5: double parens on subquery arg
SELECT abs(((SELECT -5)));

-- O6: 5 levels on subquery arg
SELECT abs(((((SELECT -5)))));

-- O7: coalesce with parenthesized args
SELECT coalesce((NULL), ((1)), (((2))));

-- O8: nullif with deep parens
SELECT nullif(((1)), ((2)));

-- O9: greatest/least with deep parens
SELECT greatest(((1)), ((2)), (((3))));
SELECT least(((3)), ((2)), (((1))));

-- ============================================================================
-- P. Aggregate functions — parenthesized arguments
-- ============================================================================

-- P1: sum with parens
SELECT sum((val)) FROM src;

-- P2: sum with double parens
SELECT sum(((val))) FROM src;

-- P3: count with subquery expression
SELECT count(*) FROM src WHERE val > ((SELECT 2));

-- P4: aggregate with parenthesized FILTER
SELECT count(*) FILTER (WHERE ((val > 2))) FROM src;

-- P5: string_agg with parens
SELECT string_agg((label), (', ') ORDER BY (val)) FROM src;

-- ============================================================================
-- Q. HAVING clause — parenthesized expressions
-- ============================================================================

-- Q1: parenthesized HAVING condition
SELECT label, count(*) FROM src GROUP BY label HAVING (count(*) > 0);

-- Q2: double parens on HAVING
SELECT label, count(*) FROM src GROUP BY label HAVING ((count(*) > 0));

-- Q3: 5 levels on HAVING
SELECT label, count(*) FROM src GROUP BY label HAVING (((((count(*) > 0)))));

-- Q4: HAVING with parenthesized subquery
SELECT label, count(*) FROM src GROUP BY label HAVING count(*) > ((SELECT 0));

-- ============================================================================
-- R. ORDER BY / LIMIT / OFFSET — parenthesized expressions
-- ============================================================================

-- R1: parenthesized ORDER BY expression
SELECT * FROM src ORDER BY (val);

-- R2: double parens on ORDER BY
SELECT * FROM src ORDER BY ((val));

-- R3: parenthesized LIMIT
SELECT * FROM src ORDER BY val LIMIT (3);

-- R4: double parens on LIMIT
SELECT * FROM src ORDER BY val LIMIT ((3));

-- R5: 5 levels on LIMIT
SELECT * FROM src ORDER BY val LIMIT (((((3)))));

-- R6: parenthesized OFFSET
SELECT * FROM src ORDER BY val LIMIT 2 OFFSET (1);

-- R7: double parens on OFFSET
SELECT * FROM src ORDER BY val LIMIT 2 OFFSET ((1));

-- R8: LIMIT with subquery
SELECT * FROM src ORDER BY val LIMIT (SELECT 2);

-- R9: LIMIT with double-parens subquery
SELECT * FROM src ORDER BY val LIMIT ((SELECT 2));

-- R10: OFFSET with subquery
SELECT * FROM src ORDER BY val LIMIT 2 OFFSET (SELECT 1);

-- R11: OFFSET with double-parens subquery
SELECT * FROM src ORDER BY val LIMIT 2 OFFSET ((SELECT 1));

-- ============================================================================
-- S. UPDATE SET — parenthesized values and subqueries
-- ============================================================================

-- S1: parenthesized value in SET
UPDATE dst SET val = (100) WHERE id = 1;

-- S2: double parens in SET
INSERT INTO dst (val, label) VALUES (1, 'test');
UPDATE dst SET val = ((100)) WHERE label = 'test';

-- S3: subquery in SET
UPDATE dst SET val = (SELECT 200) WHERE label = 'test';

-- S4: double parens subquery in SET
UPDATE dst SET val = ((SELECT 200)) WHERE label = 'test';

-- S5: 5 levels subquery in SET
UPDATE dst SET val = (((((SELECT 300))))) WHERE label = 'test';

-- S6: parenthesized WHERE in UPDATE
UPDATE dst SET val = 400 WHERE ((label = 'test'));

-- cleanup
TRUNCATE dst;

-- ============================================================================
-- T. DELETE WHERE — parenthesized conditions
-- ============================================================================

INSERT INTO dst (val, label) VALUES (1, 'del1'), (2, 'del2'), (3, 'del3');

-- T1: parenthesized WHERE
DELETE FROM dst WHERE (val = 1);

-- T2: double parens WHERE
DELETE FROM dst WHERE ((val = 2));

-- T3: subquery in DELETE WHERE
DELETE FROM dst WHERE val = (SELECT 3);

-- T4: double parens subquery in DELETE WHERE
DELETE FROM dst WHERE val = ((SELECT 3));

-- cleanup
TRUNCATE dst;

-- ============================================================================
-- U. COPY (SELECT) — parenthesized query
-- ============================================================================

-- U1: standard COPY TO stdout
COPY (SELECT val FROM src WHERE val = 1) TO STDOUT;

-- U2: double parens
COPY ((SELECT val FROM src WHERE val = 1)) TO STDOUT;

-- U3: triple parens
COPY (((SELECT val FROM src WHERE val = 1))) TO STDOUT;

-- ============================================================================
-- V. PREPARE / EXECUTE — parenthesized queries
-- ============================================================================

-- V1: PREPARE with parenthesized query body
PREPARE paren_prep AS (SELECT val FROM src WHERE val = 1);

-- V2: execute it
EXECUTE paren_prep;

-- V3: PREPARE with double parens
PREPARE paren_prep2 AS ((SELECT val FROM src WHERE val = 1));

-- V4: execute it
EXECUTE paren_prep2;

DEALLOCATE paren_prep;
DEALLOCATE paren_prep2;

-- ============================================================================
-- W. EXPLAIN — parenthesized query
-- ============================================================================

-- W1: EXPLAIN with parenthesized select (standard)
EXPLAIN SELECT val FROM src;

-- W2: EXPLAIN of parenthesized select
EXPLAIN (SELECT val FROM src);

-- W3: EXPLAIN of double-parenthesized select
EXPLAIN ((SELECT val FROM src));

-- ============================================================================
-- X. Subquery in JOIN ON — parenthesized conditions
-- ============================================================================

-- X1: parenthesized ON condition
SELECT s1.val FROM src s1 JOIN src s2 ON (s1.id = s2.id) WHERE s1.val <= 2;

-- X2: double parens ON condition
SELECT s1.val FROM src s1 JOIN src s2 ON ((s1.id = s2.id)) WHERE s1.val <= 2;

-- X3: 5 levels ON condition
SELECT s1.val FROM src s1 JOIN src s2 ON (((((s1.id = s2.id))))) WHERE s1.val <= 2;

-- X4: subquery in ON condition
SELECT s1.val FROM src s1 JOIN src s2 ON s1.id = s2.id AND s1.val > (SELECT 1);

-- X5: double parens subquery in ON
SELECT s1.val FROM src s1 JOIN src s2 ON s1.id = s2.id AND s1.val > ((SELECT 1));

-- ============================================================================
-- Y. DEFAULT expressions — parenthesized
-- ============================================================================

-- Y1: column DEFAULT with parens
CREATE TABLE def_t1 (id serial PRIMARY KEY, val int DEFAULT (0));

-- Y2: column DEFAULT with double parens
CREATE TABLE def_t2 (id serial PRIMARY KEY, val int DEFAULT ((0)));

-- Y3: column DEFAULT with 5 levels
CREATE TABLE def_t3 (id serial PRIMARY KEY, val int DEFAULT (((((0))))));

-- Y4: column DEFAULT with expression
CREATE TABLE def_t4 (id serial PRIMARY KEY, val int DEFAULT (1 + 2));

-- Y5: column DEFAULT with double parens on expression
CREATE TABLE def_t5 (id serial PRIMARY KEY, val int DEFAULT ((1 + 2)));

-- Y6: verify defaults work
INSERT INTO def_t1 DEFAULT VALUES;
INSERT INTO def_t5 DEFAULT VALUES;
SELECT val FROM def_t1;
SELECT val FROM def_t5;

-- ============================================================================
-- Z. CHECK constraints — parenthesized expressions
-- ============================================================================

-- Z1: CHECK with parenthesized expression
CREATE TABLE chk_t1 (id int, val int CHECK ((val > 0)));

-- Z2: CHECK with double parens
CREATE TABLE chk_t2 (id int, val int CHECK (((val > 0))));

-- Z3: CHECK with 5 levels
CREATE TABLE chk_t3 (id int, val int CHECK (((((val > 0))))));

-- Z4: verify constraint works
INSERT INTO chk_t3 (id, val) VALUES (1, 5);

-- Z5: verify constraint rejects
INSERT INTO chk_t3 (id, val) VALUES (2, -1);

-- ============================================================================
-- AA. COALESCE / NULLIF / GREATEST / LEAST with subqueries
-- ============================================================================

-- AA1: COALESCE with parenthesized subquery
SELECT COALESCE((SELECT NULL::int), (SELECT 42));

-- AA2: double parens
SELECT COALESCE(((SELECT NULL::int)), ((SELECT 42)));

-- AA3: NULLIF with subqueries
SELECT NULLIF((SELECT 1), (SELECT 2));

-- AA4: double parens NULLIF
SELECT NULLIF(((SELECT 1)), ((SELECT 2)));

-- ============================================================================
-- BB. Window functions — parenthesized expressions
-- ============================================================================

-- BB1: parenthesized partition expression
SELECT val, row_number() OVER (PARTITION BY (val > 2) ORDER BY val) FROM src;

-- BB2: double parens on partition
SELECT val, row_number() OVER (PARTITION BY ((val > 2)) ORDER BY val) FROM src;

-- BB3: parenthesized ORDER BY in window
SELECT val, row_number() OVER (ORDER BY (val)) FROM src;

-- BB4: double parens ORDER BY in window
SELECT val, row_number() OVER (ORDER BY ((val))) FROM src;

-- ============================================================================
-- CC. RETURNING clause — parenthesized expressions
-- ============================================================================

INSERT INTO dst (val, label) VALUES (99, 'ret');

-- CC1: parenthesized RETURNING expression
UPDATE dst SET val = 100 WHERE label = 'ret' RETURNING (val);

-- CC2: double parens RETURNING
UPDATE dst SET val = 101 WHERE label = 'ret' RETURNING ((val));

-- CC3: subquery in RETURNING
UPDATE dst SET val = 102 WHERE label = 'ret' RETURNING (SELECT 'updated');

-- CC4: double parens subquery in RETURNING
UPDATE dst SET val = 103 WHERE label = 'ret' RETURNING ((SELECT 'updated'));

-- cleanup
TRUNCATE dst;

-- ============================================================================
-- DD. ON CONFLICT — parenthesized WHERE
-- ============================================================================

CREATE TABLE upsert_t (id int PRIMARY KEY, val int);
INSERT INTO upsert_t VALUES (1, 10);

-- DD1: ON CONFLICT DO UPDATE SET with parenthesized value
INSERT INTO upsert_t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = (EXCLUDED.val);

-- DD2: double parens
INSERT INTO upsert_t VALUES (1, 30) ON CONFLICT (id) DO UPDATE SET val = ((EXCLUDED.val));

-- DD3: ON CONFLICT WHERE with parens
INSERT INTO upsert_t VALUES (1, 40) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE (upsert_t.val < EXCLUDED.val);

-- DD4: double parens WHERE
INSERT INTO upsert_t VALUES (1, 50) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE ((upsert_t.val < EXCLUDED.val));

-- DD5: verify
SELECT * FROM upsert_t;

-- ============================================================================
-- EE. LATERAL subqueries — parenthesized
-- ============================================================================

-- EE1: standard LATERAL
SELECT s.val, lat.doubled FROM src s, LATERAL (SELECT s.val * 2 AS doubled) lat WHERE s.val <= 2;

-- EE2: LATERAL with double parens
SELECT s.val, lat.doubled FROM src s, LATERAL ((SELECT s.val * 2 AS doubled)) lat WHERE s.val <= 2;

-- EE3: LATERAL with 5 levels
SELECT s.val, lat.doubled FROM src s, LATERAL (((((SELECT s.val * 2 AS doubled))))) lat WHERE s.val <= 2;

-- ============================================================================
-- FF. TYPE CAST with parenthesized expressions
-- ============================================================================

-- FF1: parenthesized expression cast
SELECT ((1 + 2))::text;

-- FF2: 5 levels then cast
SELECT (((((1 + 2)))))::text;

-- FF3: cast of subquery result
SELECT ((SELECT 42))::text;

-- FF4: 5 levels subquery then cast
SELECT (((((SELECT 42)))))::text;

-- ============================================================================
-- GG. BETWEEN — parenthesized bounds
-- ============================================================================

-- GG1: parenthesized BETWEEN bounds
SELECT * FROM src WHERE val BETWEEN (1) AND (3);

-- GG2: double parens BETWEEN bounds
SELECT * FROM src WHERE val BETWEEN ((1)) AND ((3));

-- GG3: subquery BETWEEN bounds
SELECT * FROM src WHERE val BETWEEN (SELECT 1) AND (SELECT 3);

-- GG4: double parens subquery bounds
SELECT * FROM src WHERE val BETWEEN ((SELECT 1)) AND ((SELECT 3));

-- ============================================================================
-- HH. SIMILAR TO / LIKE — parenthesized pattern
-- ============================================================================

-- HH1: LIKE with parenthesized pattern
SELECT * FROM src WHERE label LIKE ('al%');

-- HH2: double parens LIKE pattern
SELECT * FROM src WHERE label LIKE (('al%'));

-- HH3: SIMILAR TO with parens
SELECT * FROM src WHERE label SIMILAR TO ('(alpha|beta)');

-- ============================================================================
-- II. IS [NOT] NULL / IS [NOT] DISTINCT FROM — parenthesized
-- ============================================================================

-- II1: parenthesized IS NULL
SELECT (NULL) IS NULL;

-- II2: double parens IS NULL
SELECT ((NULL)) IS NULL;

-- II3: IS DISTINCT FROM with parens
SELECT (1) IS DISTINCT FROM (2);

-- II4: double parens IS DISTINCT FROM
SELECT ((1)) IS DISTINCT FROM ((2));

-- II5: IS NOT DISTINCT FROM with parens
SELECT ((1)) IS NOT DISTINCT FROM ((1));

-- ============================================================================
-- JJ. PL/pgSQL — parenthesized expressions in function bodies
-- ============================================================================

-- JJ1: assignment with parenthesized expression
CREATE FUNCTION paren_plpgsql_assign() RETURNS int LANGUAGE plpgsql AS $$
DECLARE v int;
BEGIN
    v := ((1 + 2));
    RETURN v;
END;
$$;
SELECT paren_plpgsql_assign();

-- JJ2: IF condition with deep parens
CREATE FUNCTION paren_plpgsql_if(x int) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
    IF (((((x > 0))))) THEN
        RETURN 'positive';
    ELSE
        RETURN 'non-positive';
    END IF;
END;
$$;
SELECT paren_plpgsql_if(5);
SELECT paren_plpgsql_if(-1);

-- JJ3: RETURN with parenthesized subquery
CREATE FUNCTION paren_plpgsql_ret_sub() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
    RETURN ((SELECT 42));
END;
$$;
SELECT paren_plpgsql_ret_sub();

-- JJ4: WHILE condition with deep parens
CREATE FUNCTION paren_plpgsql_while() RETURNS int LANGUAGE plpgsql AS $$
DECLARE i int := 0;
BEGIN
    WHILE (((i < 5))) LOOP
        i := i + 1;
    END LOOP;
    RETURN i;
END;
$$;
SELECT paren_plpgsql_while();

-- JJ5: FOR loop bounds with parens
CREATE FUNCTION paren_plpgsql_for() RETURNS int LANGUAGE plpgsql AS $$
DECLARE s int := 0;
BEGIN
    FOR i IN (1) .. (5) LOOP
        s := s + i;
    END LOOP;
    RETURN s;
END;
$$;
SELECT paren_plpgsql_for();

-- JJ6: RAISE with parenthesized expression
CREATE FUNCTION paren_plpgsql_raise() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'val=%', ((1 + 2));
END;
$$;
SELECT paren_plpgsql_raise();

-- ============================================================================
-- KK. SQL function body — parenthesized
-- ============================================================================

-- KK1: SQL function with parenthesized return expression
CREATE FUNCTION paren_sql_fn(x int) RETURNS int LANGUAGE sql AS $$
    SELECT ((x * 2));
$$;
SELECT paren_sql_fn(21);

-- KK2: SQL function body with subquery
CREATE FUNCTION paren_sql_sub() RETURNS int LANGUAGE sql AS $$
    SELECT ((SELECT 99));
$$;
SELECT paren_sql_sub();

-- ============================================================================
-- LL. DO block — parenthesized expressions
-- ============================================================================

-- LL1: DO block with parenthesized expression
DO $$ BEGIN PERFORM ((1 + 2)); END $$;

-- LL2: DO block with parenthesized subquery
DO $$ BEGIN PERFORM ((SELECT 42)); END $$;

-- ============================================================================
-- MM. CREATE INDEX — parenthesized expressions
-- ============================================================================

-- MM1: expression index with parens
CREATE INDEX paren_idx1 ON src ((val + 1));

-- MM2: expression index with double parens
CREATE INDEX paren_idx2 ON src (((val + 1)));

-- MM3: expression index with 5 levels
CREATE INDEX paren_idx3 ON src (((((val + 1)))));

-- MM4: partial index WHERE with parens
CREATE INDEX paren_idx4 ON src (val) WHERE ((val > 2));

-- MM5: partial index WHERE with 5 levels
CREATE INDEX paren_idx5 ON src (val) WHERE (((((val > 2)))));

-- ============================================================================
-- NN. GENERATED columns — parenthesized expressions
-- ============================================================================

-- NN1: GENERATED with parenthesized expression
CREATE TABLE gen_t1 (a int, b int GENERATED ALWAYS AS ((a * 2)) STORED);

-- NN2: double parens
CREATE TABLE gen_t2 (a int, b int GENERATED ALWAYS AS (((a * 2))) STORED);

-- NN3: 5 levels
CREATE TABLE gen_t3 (a int, b int GENERATED ALWAYS AS (((((a * 2))))) STORED);

-- NN4: verify
INSERT INTO gen_t3 VALUES (10);
SELECT * FROM gen_t3;

-- ============================================================================
-- OO. DOMAIN constraints — parenthesized CHECK
-- ============================================================================

-- OO1: domain CHECK with parens
CREATE DOMAIN paren_dom1 AS int CHECK (((VALUE > 0)));

-- OO2: 5 levels
CREATE DOMAIN paren_dom2 AS int CHECK (((((VALUE > 0)))));

-- OO3: verify
SELECT 5::paren_dom2;

-- ============================================================================
-- PP. INSERT ... ON CONFLICT target — parenthesized expressions
-- ============================================================================

-- PP1: ON CONFLICT with parenthesized column
INSERT INTO upsert_t VALUES (2, 100) ON CONFLICT ((id)) DO UPDATE SET val = EXCLUDED.val;

-- PP2: verify
SELECT * FROM upsert_t WHERE id = 2;

-- ============================================================================
-- QQ. Subquery comparison operators — parenthesized
-- ============================================================================

-- QQ1: scalar subquery comparison
SELECT * FROM src WHERE val > (SELECT 2);

-- QQ2: double parens
SELECT * FROM src WHERE val > ((SELECT 2));

-- QQ3: 5 levels
SELECT * FROM src WHERE val > (((((SELECT 2)))));

-- QQ4: multiple subquery comparisons with deep parens
SELECT * FROM src WHERE val > ((SELECT 1)) AND val < ((SELECT 4));

-- ============================================================================
-- RR. MERGE — parenthesized subqueries in USING and conditions
-- ============================================================================

CREATE TABLE merge_target (id int PRIMARY KEY, val int);
INSERT INTO merge_target VALUES (1, 10), (2, 20);

CREATE TABLE merge_source (id int PRIMARY KEY, val int);
INSERT INTO merge_source VALUES (2, 200), (3, 300);

-- RR1: MERGE with parenthesized USING subquery
MERGE INTO merge_target t
USING ((SELECT * FROM merge_source)) s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val);

-- RR2: verify
SELECT * FROM merge_target ORDER BY id;

-- ============================================================================
-- SS. Complex nesting — combining multiple parenthesized contexts
-- ============================================================================

-- SS1: deeply nested expression tree
SELECT (((((1 + (((2 * (((3))))))))));

-- SS2: subquery in subquery in subquery, all parenthesized
SELECT ((SELECT ((SELECT ((SELECT 42))))));

-- SS3: INSERT with parenthesized SELECT containing parenthesized subquery in WHERE
INSERT INTO dst (val, label) ((SELECT val, label FROM src WHERE val = ((SELECT min(val) FROM src))));

-- SS4: CTE with parenthesized body containing parenthesized subquery
WITH cte AS ((SELECT val FROM src WHERE val > ((SELECT 2))))
SELECT count(*) FROM cte;

-- SS5: view over parenthesized subquery with parenthesized expression
CREATE VIEW paren_complex_v AS ((SELECT ((val * 2)) AS doubled FROM src));
SELECT * FROM paren_complex_v ORDER BY doubled;

-- SS6: correlated subquery with deep parens
SELECT s1.val,
       ((SELECT count(*) FROM src s2 WHERE ((s2.val <= s1.val))))
FROM src s1
ORDER BY s1.val;

-- SS7: 10-level paren on literal in INSERT VALUES
INSERT INTO dst (val, label) VALUES (((((((((((42))))))))))), 'deep');

-- SS8: verify
SELECT val, label FROM dst WHERE label = 'deep';

-- cleanup
TRUNCATE dst;

-- ============================================================================
-- TT. Parenthesized table references (rare but valid PG syntax)
-- ============================================================================

-- TT1: parenthesized table name in FROM
SELECT * FROM (src) WHERE val <= 2;

-- TT2: double parens around table name
SELECT * FROM ((src)) WHERE val <= 2;

-- ============================================================================
-- UU. VALUES as standalone — parenthesized
-- ============================================================================

-- UU1: standalone VALUES
VALUES (1, 'a'), (2, 'b');

-- UU2: parenthesized standalone VALUES
(VALUES (1, 'a'), (2, 'b'));

-- UU3: double parens around VALUES
((VALUES (1, 'a'), (2, 'b')));

-- ============================================================================
-- VV. DECLARE CURSOR — parenthesized query
-- ============================================================================

BEGIN;

-- VV1: standard cursor
DECLARE paren_cur1 CURSOR FOR SELECT val FROM src;
FETCH 1 FROM paren_cur1;
CLOSE paren_cur1;

-- VV2: parenthesized cursor query
DECLARE paren_cur2 CURSOR FOR (SELECT val FROM src);
FETCH 1 FROM paren_cur2;
CLOSE paren_cur2;

-- VV3: double parens cursor query
DECLARE paren_cur3 CURSOR FOR ((SELECT val FROM src));
FETCH 1 FROM paren_cur3;
CLOSE paren_cur3;

COMMIT;

-- ============================================================================
-- WW. CREATE MATERIALIZED VIEW — parenthesized
-- ============================================================================

-- WW1: standard
CREATE MATERIALIZED VIEW paren_mv1 AS SELECT val FROM src;

-- WW2: single parens
CREATE MATERIALIZED VIEW paren_mv2 AS (SELECT val FROM src);

-- WW3: double parens
CREATE MATERIALIZED VIEW paren_mv3 AS ((SELECT val FROM src));

-- WW4: 5 levels
CREATE MATERIALIZED VIEW paren_mv4 AS (((((SELECT val FROM src)))));

-- WW5: verify
SELECT count(*) AS cnt FROM paren_mv4;

-- ============================================================================
-- XX. GROUP BY / DISTINCT ON — parenthesized expressions
-- ============================================================================

-- XX1: GROUP BY with parens
SELECT (val), count(*) FROM src GROUP BY (val) ORDER BY val;

-- XX2: GROUP BY with double parens
SELECT val, count(*) FROM src GROUP BY ((val)) ORDER BY val;

-- XX3: DISTINCT ON with parens
SELECT DISTINCT ON ((val)) val, label FROM src ORDER BY val, label;

-- ============================================================================
-- YY. ARRAY subscript / slice — parenthesized index
-- ============================================================================

-- YY1: parenthesized array index
SELECT (ARRAY[10,20,30])[(1)];

-- YY2: double parens array index
SELECT (ARRAY[10,20,30])[((1))];

-- YY3: parenthesized array slice bounds
SELECT (ARRAY[10,20,30,40,50])[(2):((4))];

-- ============================================================================
-- ZZ. JSON/JSONB operators — parenthesized expressions
-- ============================================================================

-- ZZ1: parenthesized jsonb value
SELECT ((('{"a":1}'::jsonb)))->'a';

-- ZZ2: parenthesized key
SELECT '{"a":1}'::jsonb->((('a')));

-- ZZ3: parenthesized jsonb in WHERE
SELECT * FROM src WHERE ((('{"x":true}'::jsonb))) @> '{"x":true}';

-- ============================================================================
-- AAA. Deeply nested arithmetic — 10+ levels
-- ============================================================================

-- AAA1: 10-level nested addition
SELECT ((((((((((1 + 2))))))))));

-- AAA2: 10-level nested in WHERE
SELECT * FROM src WHERE val = ((((((((((3))))))))));

-- AAA3: 15-level nested literal
SELECT (((((((((((((((42)))))))))))))));

-- AAA4: 20-level nested literal
SELECT ((((((((((((((((((((1))))))))))))))))))));

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA paren_test CASCADE;
