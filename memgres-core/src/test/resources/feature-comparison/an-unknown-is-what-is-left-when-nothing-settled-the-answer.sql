-- ============================================================================
-- An unknown is what is left when nothing settled the answer
--
-- A comparison against a set is settled by the set. An empty one settles it without comparing
-- anything -- IN over nothing is false and ALL over nothing is true -- so a null on the left leaves
-- nothing there to be unknown about. Only once there is something to compare against does a null
-- make a comparison unknown, and even then a comparison that came out false settles the answer
-- whatever the unknown ones did.
--
-- BETWEEN is shorthand for a pair of comparisons joined by AND, and that AND is the same one every
-- other pair is joined by: 1 is below 2 whatever the upper bound is, so 1 BETWEEN 2 AND NULL is
-- false rather than unknown. SYMMETRIC is the same pair written twice and joined by OR.
--
-- A row is compared as a row rather than as a value: two rows are equal when every pair of members
-- is equal, unequal when some pair is unequal, and unknown otherwise. A null member does not settle
-- the answer by itself, and a pair that differs settles it even when another pair is null.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE tv_a (x int);
INSERT INTO tv_a VALUES (1), (2), (NULL);

-- ============================================================================
-- A pair of comparisons, joined the way every pair is
-- ============================================================================
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (1 BETWEEN 2 AND NULL)::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (3 BETWEEN NULL AND 2)::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (1 NOT BETWEEN 2 AND NULL)::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (3 NOT BETWEEN NULL AND 2)::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (1 BETWEEN NULL AND 3)::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (2 BETWEEN 1 AND NULL)::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL BETWEEN 1 AND 2)::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL BETWEEN NULL AND NULL)::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (2 BETWEEN 1 AND 3)::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (4 BETWEEN 1 AND 3)::text;

-- SYMMETRIC is the same pair written both ways round and joined by OR.
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (1 BETWEEN SYMMETRIC 2 AND NULL)::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (3 BETWEEN SYMMETRIC NULL AND 2)::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (2 BETWEEN SYMMETRIC 3 AND 1)::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (4 BETWEEN SYMMETRIC 3 AND 1)::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (4 NOT BETWEEN SYMMETRIC 3 AND 1)::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (1 BETWEEN SYMMETRIC NULL AND 3)::text;

-- And the text of a value is still read as the type it is compared against.
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ('2' BETWEEN 1 AND 3)::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (2 BETWEEN '1' AND NULL)::text;

-- ============================================================================
-- An empty set settles the answer without comparing anything
-- ============================================================================
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (NULL::int IN (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (NULL::int NOT IN (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (NULL::int = ANY (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (NULL::int = ALL (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (NULL::int > ANY (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (NULL::int > ALL (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (NULL::int = ANY ('{}'::int[]))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (NULL::int = ALL ('{}'::int[]))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (1 IN (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (1 NOT IN (SELECT 1 WHERE false))::text;

-- Which is a row kept or dropped by a WHERE, not a curiosity of a select list.
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM tv_a WHERE x NOT IN (SELECT 1 WHERE false);
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM tv_a WHERE x IN (SELECT 1 WHERE false);
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM tv_a WHERE x NOT IN (SELECT 1);
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM tv_a WHERE x IN (SELECT 1);

-- ============================================================================
-- Once something is there to compare against, a null is unknown again
-- ============================================================================
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int IN (SELECT 1))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int NOT IN (SELECT 1))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int = ANY (SELECT 1))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int = ALL (SELECT 1))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int IN (1, 2))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int NOT IN (1, 2))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int = ANY (ARRAY[1, 2]))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (NULL::int = ALL (ARRAY[1, 2]))::text;

-- And a comparison that came out false settles it whatever the unknown ones did.
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (1 = ANY (SELECT v FROM (VALUES (NULL::int), (1)) t(v)))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (1 = ALL (SELECT v FROM (VALUES (NULL::int), (2)) t(v)))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (1 = ANY (SELECT v FROM (VALUES (NULL::int), (2)) t(v)))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (1 IN (SELECT v FROM (VALUES (NULL::int), (1)) t(v)))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (1 NOT IN (SELECT v FROM (VALUES (NULL::int), (2)) t(v)))::text;

-- ============================================================================
-- A row is compared as a row
-- ============================================================================
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (ROW(1, NULL) IN (ROW(1, 2)))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (ROW(1, NULL) IN (ROW(1, 2), ROW(1, NULL)))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (ROW(1, NULL) IN (ROW(2, 2)))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (ROW(NULL::int, 1) IN (ROW(1, 2), ROW(2, 1)))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (ROW(NULL::int, 1) NOT IN (ROW(1, 2)))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT (ROW(1, NULL) = ROW(1, 2))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (ROW(1, NULL) = ROW(2, 2))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (ROW(1, NULL) <> ROW(2, 2))::text;

-- The same rows, read out of a subquery instead of written out.
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT ((NULL::int, 1) IN (SELECT 1, 2))::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((NULL::int, 1) NOT IN (SELECT 1, 2))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT ((NULL::int, 1) IN (SELECT 1, 1))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT ((1, NULL::int) IN (SELECT 1, 2))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT ((1, NULL::int) IN (SELECT 2, 2))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT ((NULL::int, 1) IN (SELECT v, w FROM (VALUES (1,2),(2,1)) t(v,w)))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT ((NULL::int, 1) IN (SELECT v, w FROM (VALUES (1,2),(2,2)) t(v,w)))::text;
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT ((NULL::int, 1) IN (SELECT v, w FROM (VALUES (1,1)) t(v,w)))::text;

-- An array is one value, and two arrays holding a null in the same place are the same array.
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (ARRAY[1, NULL] = ARRAY[1, NULL])::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (ARRAY[1, NULL] = ARRAY[1, 2])::text;
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (ARRAY[1, NULL] IN (ARRAY[1, NULL]))::text;
-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT (ARRAY[1, NULL] IN (ARRAY[1, 2]))::text;

-- teardown
DROP TABLE tv_a;
