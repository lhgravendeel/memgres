-- ============================================================================
-- what a query means is what the grammar says it means
--
-- Precedence, chaining and the shapes a select may take. Each case below asks what
-- PostgreSQL reads out of a written query, not what the words look like they mean.
--
-- ============================================================================

-- setup
CREATE TABLE zz_z1 (id int, x int, a int);
INSERT INTO zz_z1 VALUES (1, 10, 5), (2, 20, 6);
CREATE TABLE zz_z2 (id int, y int);
INSERT INTO zz_z2 VALUES (1, 100), (3, 300);
CREATE TABLE zz_z3 (t text);
INSERT INTO zz_z3 VALUES ('p'), ('q');

-- ============================================================================
-- how far to the left a membership test reaches
-- ============================================================================
-- IN binds tighter than =, so the comparison sees the test's answer
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 = 1 IN (true) AS c1;
-- and the answer of a test is a boolean, whatever the operands were
-- begin-expected
-- columns: c2
-- row: t
-- end-expected
SELECT 1 IN (1) = true AS c2;
-- begin-expected
-- columns: c3
-- row: t
-- end-expected
SELECT 1 IN (2) = false AS c3;
-- a test may be followed by another test
-- begin-expected
-- columns: c4
-- row: t
-- end-expected
SELECT 1 IN (1) IN (true) AS c4;
-- NOT stands outside the test
-- begin-expected
-- columns: c5
-- row: t
-- end-expected
SELECT NOT 1 IN (2) AS c5;
-- AND stands outside NOT
-- begin-expected
-- columns: c6
-- row: t
-- end-expected
SELECT NOT 1 IN (2) AND true AS c6;

-- ============================================================================
-- an IN list is resolved whole, before it is searched
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 IN (1, true) AS c7;

-- ============================================================================
-- what a range test and a pattern test bind to
-- ============================================================================
-- begin-expected
-- columns: c8
-- row: t
-- end-expected
SELECT 2 BETWEEN 1 AND 3 = true AS c8;
-- begin-expected
-- columns: c9
-- row: t
-- end-expected
SELECT 'ab' LIKE 'a%' = true AS c9;
-- begin-expected
-- columns: c10
-- row: t
-- end-expected
SELECT 'AB' ILIKE 'a%' = true AS c10;
-- a range test may not be followed by another
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT 1 BETWEEN 0 AND 2 BETWEEN 0 AND 2 AS c11;

-- ============================================================================
-- ANY and ALL end in a closing paren, so a comparison may follow
-- ============================================================================
-- begin-expected
-- columns: c12
-- row: t
-- end-expected
SELECT 1 = ANY(ARRAY[1,2]) = true AS c12;
-- begin-expected
-- columns: c13
-- row: t
-- end-expected
SELECT 1 = ALL(ARRAY[1,1]) = true AS c13;

-- ============================================================================
-- the inheritance star, and where it may be written
-- ============================================================================
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM zz_z1 * ORDER BY id;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM ONLY zz_z1 ORDER BY id;
-- begin-expected
-- columns: id | y
-- row: 1 | 100
-- row: 3 | 300
-- end-expected
TABLE ONLY zz_z2;
-- ONLY takes no star
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM ONLY zz_z1 * a;

-- ============================================================================
-- LATERAL takes a subquery or a function, not a relation
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT * FROM LATERAL zz_z1;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT * FROM zz_z2, LATERAL zz_z1;

-- ============================================================================
-- a USING clause may be given a name
-- ============================================================================
-- the name answers with the merged columns and nothing else
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT j.id FROM zz_z1 a JOIN zz_z2 b USING (id) AS j;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT j.* FROM zz_z1 a JOIN zz_z2 b USING (id) AS j;
-- the relations behind it go on answering to their own names
-- begin-expected
-- columns: x | y | id
-- row: 10 | 100 | 1
-- end-expected
SELECT a.x, b.y, j.id FROM zz_z1 a JOIN zz_z2 b USING (id) AS j;
-- and what the join exposes is unchanged
-- begin-expected
-- columns: id | x | a | y
-- row: 1 | 10 | 5 | 100
-- end-expected
SELECT * FROM zz_z1 a JOIN zz_z2 b USING (id) AS j;
-- a column the clause did not merge is a column the name has not got
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT j.x FROM zz_z1 a JOIN zz_z2 b USING (id) AS j;
-- the name is only taken after AS
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT j.id FROM zz_z1 a JOIN zz_z2 b USING (id) j;

-- ============================================================================
-- an ordering operator has to order, and has to exist
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 5
-- row: 6
-- end-expected
SELECT a FROM zz_z1 ORDER BY a USING <;
-- begin-expected
-- columns: a
-- row: 6
-- row: 5
-- end-expected
SELECT a FROM zz_z1 ORDER BY a USING > ;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING <=;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING >=;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING =;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING <>;
-- an operator nothing defines for the type being sorted is a different complaint
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING @@;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING @>;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING ||;
-- and one that does exist for it has still not got the property a sort asks of it
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT t FROM zz_z3 ORDER BY t USING @@;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT t FROM zz_z3 ORDER BY t USING ~~;
-- the sort is read item by item
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING <, x USING @@;
-- NULLS may still be written after one
-- begin-expected
-- columns: a
-- row: 5
-- row: 6
-- end-expected
SELECT a FROM zz_z1 ORDER BY a USING < NULLS FIRST;

-- ============================================================================
-- every row of a VALUES list is the same relation's row
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
VALUES (1,2),(3);
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
VALUES (1),(2,3);
-- begin-expected
-- columns: column1 | column2
-- row: 1 | 2
-- row: 3 | 4
-- end-expected
VALUES (1,2),(3,4);

-- ============================================================================
-- a relation's column has a type, so an unknown one is settled as text
-- ============================================================================
-- begin-expected
-- columns: t1
-- row: text
-- row: text
-- end-expected
SELECT pg_typeof(column1)::text AS t1 FROM (VALUES (NULL),(NULL)) v;
-- begin-expected
-- columns: t2
-- row: text
-- end-expected
SELECT pg_typeof(column1)::text AS t2 FROM (VALUES (NULL)) v;
-- begin-expected
-- columns: t3
-- row: text
-- row: text
-- end-expected
SELECT pg_typeof(column1)::text AS t3 FROM (VALUES (NULL),('x')) v;
-- begin-expected
-- columns: t4
-- row: text
-- end-expected
SELECT pg_typeof(q)::text AS t4 FROM (SELECT NULL AS q) s;
-- begin-expected
-- columns: t5
-- row: text
-- row: text
-- end-expected
SELECT pg_typeof(q)::text AS t5 FROM (SELECT NULL AS q UNION ALL SELECT NULL) s;
-- but a written NULL that no relation holds is unknown still
-- begin-expected
-- columns: t6
-- row: unknown
-- end-expected
SELECT pg_typeof(NULL)::text AS t6;


-- teardown
DROP TABLE zz_z1;
DROP TABLE zz_z2;
DROP TABLE zz_z3;
