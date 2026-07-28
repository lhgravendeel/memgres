-- ============================================================================
-- Feature Comparison: clause-level validation of a query
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Before it reads a row, PostgreSQL decides whether the query has a defined
-- answer at all: which expressions a grouped query may name, whether DISTINCT
-- ON picks a determined row, whether two FROM items can be told apart, whether
-- a join condition is a predicate, and where a write may appear. Each check
-- left unmade turns a query PostgreSQL refuses into one that returns an
-- arbitrary answer -- or, for the nested writes, quietly changes data.
-- ============================================================================

DROP TABLE IF EXISTS qcv_t CASCADE;
DROP TABLE IF EXISTS qcv_a CASCADE;
DROP TABLE IF EXISTS qcv_b CASCADE;
DROP TABLE IF EXISTS qcv_ins CASCADE;
DROP FUNCTION IF EXISTS qcv_setof_rec() CASCADE;
DROP FUNCTION IF EXISTS qcv_out_fn() CASCADE;

CREATE TABLE qcv_t (i int, g int, v int, s text);
INSERT INTO qcv_t VALUES (1,1,10,'a'),(2,1,20,NULL),(3,2,30,'c'),(4,2,40,'d');
CREATE TABLE qcv_a (id int, x int);
INSERT INTO qcv_a VALUES (1,10),(2,20),(3,30);
CREATE TABLE qcv_b (id int, y int);
INSERT INTO qcv_b VALUES (1,100),(2,200),(3,300);
CREATE TABLE qcv_ins (i int PRIMARY KEY, j int);
CREATE FUNCTION qcv_setof_rec() RETURNS SETOF record AS $$ SELECT 1, 2 $$ LANGUAGE sql;
CREATE FUNCTION qcv_out_fn(OUT a int, OUT b text) RETURNS SETOF record
  AS $$ SELECT 1, 'x' $$ LANGUAGE sql;

-- ============================================================================
-- 1. Aggregate placement
-- ============================================================================

-- HAVING is evaluated once per group, so a bare column there is as ungrouped
-- as one in the select list
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT count(*) FROM qcv_t HAVING i > 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT g, count(*) FROM qcv_t GROUP BY g HAVING i > 0;

-- An aggregate in ORDER BY groups the whole query, leaving i ungrouped
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT i FROM qcv_t ORDER BY sum(v);

-- An aggregate consumes one row at a time; a set-returning argument has no meaning
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT count(generate_series(1,3));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT sum(generate_series(1,3));

-- A NULL key would silently drop the row from the object
-- begin-expected-error
-- sqlstate: 22004
-- message-like: null value not allowed for object key
-- end-expected-error
SELECT json_object_agg(s, i) FROM qcv_t;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: field name must not be null
-- end-expected-error
SELECT jsonb_object_agg(s, i) FROM qcv_t;

-- Neighbouring aggregate behaviour is unchanged
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM qcv_t HAVING count(*) > 0;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM qcv_t HAVING sum(v) > 0;

-- begin-expected
-- columns: g
-- row: 2
-- row: 1
-- end-expected
SELECT g FROM qcv_t GROUP BY g ORDER BY sum(v) DESC;

-- begin-expected
-- columns: sum
-- row: 100
-- end-expected
SELECT sum(v) FROM qcv_t ORDER BY sum(v);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT 1 AS n FROM qcv_t ORDER BY sum(v);

-- begin-expected
-- columns: jsonb_object_agg
-- row: {"a": 1, "c": 3, "d": 4}
-- end-expected
SELECT jsonb_object_agg(s, i) FROM qcv_t WHERE s IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM (SELECT generate_series(1,3)) z;

-- ============================================================================
-- 2. DISTINCT ON must lead the ORDER BY
-- ============================================================================

-- Which row of each group survives is otherwise undefined
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: SELECT DISTINCT ON expressions must match initial ORDER BY expressions
-- end-expected-error
SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY i;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: SELECT DISTINCT ON expressions must match initial ORDER BY expressions
-- end-expected-error
SELECT DISTINCT ON (i) i FROM qcv_t ORDER BY g;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: SELECT DISTINCT ON expressions must match initial ORDER BY expressions
-- end-expected-error
SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY 1;

-- A matching prefix, an absent ORDER BY, an alias and a qualified name are all fine
-- begin-expected
-- columns: i
-- row: 1
-- row: 3
-- end-expected
SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY g, i;

-- begin-expected
-- columns: i
-- row: 3
-- row: 1
-- end-expected
SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY g DESC, i;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT DISTINCT ON (g, i) i FROM qcv_t ORDER BY g, i;

-- begin-expected
-- columns: i
-- row: 1
-- row: 3
-- end-expected
SELECT DISTINCT ON (g+0) i FROM qcv_t ORDER BY g+0, i;

-- begin-expected
-- columns: grp | i
-- row: 1, 1
-- row: 2, 3
-- end-expected
SELECT DISTINCT ON (g) g AS grp, i FROM qcv_t ORDER BY grp, i;

-- begin-expected
-- columns: i
-- row: 1
-- row: 3
-- end-expected
SELECT DISTINCT ON (qcv_t.g) i FROM qcv_t ORDER BY g;

-- ============================================================================
-- 3. Two FROM items may not answer to the same name
-- ============================================================================

-- An unaliased self join is a common typo; any qualified reference would
-- resolve to one of the two arbitrarily
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "qcv_a" specified more than once
-- end-expected-error
SELECT count(*) FROM qcv_a JOIN qcv_a ON true;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "qcv_a" specified more than once
-- end-expected-error
SELECT count(*) FROM qcv_a, qcv_a;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "t1" specified more than once
-- end-expected-error
SELECT count(*) FROM qcv_a t1, qcv_b t1;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: specified more than once
-- end-expected-error
SELECT count(*) FROM qcv_a, (SELECT 1) qcv_a;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: specified more than once
-- end-expected-error
SELECT count(*) FROM public.qcv_a, qcv_a;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: specified more than once
-- end-expected-error
SELECT count(*) FROM generate_series(1,2), generate_series(1,2);

-- Distinct names are unaffected
-- begin-expected
-- columns: count
-- row: 9
-- end-expected
SELECT count(*) FROM qcv_a a1 JOIN qcv_a a2 ON true;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM qcv_a JOIN (SELECT 1 AS id) qcv_b ON true;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (SELECT 1 AS n) SELECT count(*) FROM w w1, w w2;

-- ============================================================================
-- 4. Join conditions
-- ============================================================================

-- A non-boolean ON was read as always-true and produced the full cross product
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean
-- end-expected-error
SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean
-- end-expected-error
SELECT count(*) FROM qcv_a LEFT JOIN qcv_b ON qcv_a.id;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in JOIN conditions
-- end-expected-error
SELECT count(*) FROM qcv_a JOIN qcv_b ON count(*) > 0;

-- begin-expected-error
-- sqlstate: 42701
-- message-like: appears more than once in USING clause
-- end-expected-error
SELECT count(*) FROM qcv_a JOIN qcv_b USING (id, id);

-- Ordinary join conditions are unaffected
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM qcv_a JOIN qcv_b USING (id);

-- begin-expected
-- columns: count
-- row: 9
-- end-expected
SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id > 0 AND qcv_b.id > 0;

-- begin-expected
-- columns: count
-- row: 9
-- end-expected
SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id::boolean;

-- ============================================================================
-- 5. Ambiguity reaching through a join alias
-- ============================================================================

-- The alias exposes both id columns under one name
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "id" is ambiguous
-- end-expected-error
SELECT count(*) FROM (qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id) AS j WHERE j.id = 2;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "id" is ambiguous
-- end-expected-error
SELECT count(*) FROM (SELECT * FROM qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id) AS j
WHERE j.id = 2;

-- A name exposed once still resolves, and USING merges the pair into one column
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id) AS j WHERE j.x = 10;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (qcv_a JOIN qcv_b USING (id)) AS j WHERE j.id = 2;

-- ============================================================================
-- 6. LATERAL across the nullable side of a join
-- ============================================================================

-- The left side's rows are not determined when the lateral item is evaluated
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: invalid reference to FROM-clause entry for table "t"
-- end-expected-error
SELECT count(*) FROM qcv_a t RIGHT JOIN LATERAL (SELECT t.x) s ON true;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: invalid reference to FROM-clause entry for table "t"
-- end-expected-error
SELECT count(*) FROM qcv_a t FULL JOIN LATERAL (SELECT t.x) s ON true;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: invalid reference to FROM-clause entry for table "t"
-- end-expected-error
SELECT count(*) FROM qcv_a t RIGHT JOIN generate_series(1, t.x) s ON true;

-- INNER and LEFT can see the left side, and a lateral item that reads nothing is fine
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM qcv_a t LEFT JOIN LATERAL (SELECT t.x) s ON true;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM qcv_a t, LATERAL (SELECT t.x) s;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM qcv_a t RIGHT JOIN LATERAL (SELECT 1 AS z) s ON true;

-- ============================================================================
-- 7. WITHIN GROUP belongs to the ordered-set aggregates
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(integer, integer) does not exist
-- end-expected-error
SELECT sum(v) WITHIN GROUP (ORDER BY v) FROM qcv_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: is not an ordered-set aggregate
-- end-expected-error
SELECT count(*) WITHIN GROUP (ORDER BY 1) FROM qcv_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: does not exist
-- end-expected-error
SELECT string_agg(s, ',') WITHIN GROUP (ORDER BY i) FROM qcv_t;

-- The real ordered-set aggregates are unaffected
-- begin-expected
-- columns: percentile_cont
-- row: 25
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM qcv_t;

-- begin-expected
-- columns: mode
-- row: 10
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY v) FROM qcv_t;

-- ============================================================================
-- 8. Functions in FROM
-- ============================================================================

-- A bare record result has no column names of its own
-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is required for functions returning "record"
-- end-expected-error
SELECT * FROM qcv_setof_rec();

-- OUT parameters already name the columns
-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is redundant for a function with OUT parameters
-- end-expected-error
SELECT * FROM qcv_out_fn() AS t(x int, y text);

-- The list must agree with what the body produces
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: return type mismatch in function declared to return record
-- end-expected-error
SELECT * FROM qcv_setof_rec() AS t(x int, y int, z int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: could not identify column "y" in record data type
-- end-expected-error
SELECT string_agg((qcv_setof_rec()).y, ',');

-- A matching list resolves
-- begin-expected
-- columns: x | y
-- row: 1, 2
-- end-expected
SELECT * FROM qcv_setof_rec() AS t(x int, y int);

-- begin-expected
-- columns: a | b
-- row: 1, x
-- end-expected
SELECT * FROM qcv_out_fn();

-- Any function may appear in FROM: a scalar is a one-row set
-- begin-expected
-- columns: abs
-- row: 3
-- end-expected
SELECT * FROM abs(-3);

-- begin-expected
-- columns: u
-- row: HI
-- end-expected
SELECT * FROM upper('hi') AS t(u);

-- begin-expected
-- columns: v | o
-- row: 1, 1
-- end-expected
SELECT * FROM abs(1) WITH ORDINALITY AS t(v, o);

-- begin-expected
-- columns: v | o
-- row: 1, 1
-- row: 2, 2
-- row: 3, 3
-- end-expected
SELECT * FROM generate_series(1,3) WITH ORDINALITY AS t(v, o);

-- ============================================================================
-- 9. RETURNING reports one row at a time
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in RETURNING
-- end-expected-error
INSERT INTO qcv_ins VALUES (1,1) RETURNING count(*);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in RETURNING
-- end-expected-error
INSERT INTO qcv_ins VALUES (2,2) RETURNING row_number() OVER ();

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in RETURNING
-- end-expected-error
UPDATE qcv_ins SET j = 2 RETURNING count(*);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in RETURNING
-- end-expected-error
DELETE FROM qcv_ins RETURNING count(*);

-- Nothing above was written
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM qcv_ins;

-- Ordinary RETURNING is unaffected
-- begin-expected
-- columns: i
-- row: 5
-- end-expected
INSERT INTO qcv_ins VALUES (5,5) RETURNING i;

-- begin-expected
-- columns: j
-- row: 6
-- end-expected
UPDATE qcv_ins SET j = 6 RETURNING j;

-- begin-expected
-- columns: i
-- row: 5
-- end-expected
DELETE FROM qcv_ins RETURNING i;

-- ============================================================================
-- 10. A write only belongs in a top-level CTE
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "INTO"
-- end-expected-error
SELECT * FROM (INSERT INTO qcv_ins VALUES (9,9) RETURNING j) x;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SET"
-- end-expected-error
SELECT * FROM (UPDATE qcv_ins SET j=1 RETURNING i) x;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FROM"
-- end-expected-error
SELECT * FROM (DELETE FROM qcv_ins RETURNING i) x;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: WITH clause containing a data-modifying statement must be at the top level
-- end-expected-error
SELECT 1 WHERE EXISTS (
  WITH x AS (INSERT INTO qcv_ins VALUES (8,8) RETURNING i) SELECT 1 FROM x);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: WITH clause containing a data-modifying statement must be at the top level
-- end-expected-error
WITH a AS (WITH b AS (INSERT INTO qcv_ins VALUES (21,21) RETURNING i) SELECT * FROM b)
SELECT * FROM a;

-- Nothing above was written
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM qcv_ins;

-- The top-level form is exactly what PostgreSQL allows, and it still runs
-- begin-expected
-- columns: i
-- row: 7
-- end-expected
WITH x AS (INSERT INTO qcv_ins VALUES (7,7) RETURNING i) SELECT * FROM x;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM qcv_ins;

DROP FUNCTION IF EXISTS qcv_setof_rec() CASCADE;
DROP FUNCTION IF EXISTS qcv_out_fn() CASCADE;
DROP TABLE IF EXISTS qcv_ins CASCADE;
DROP TABLE IF EXISTS qcv_b CASCADE;
DROP TABLE IF EXISTS qcv_a CASCADE;
DROP TABLE IF EXISTS qcv_t CASCADE;
