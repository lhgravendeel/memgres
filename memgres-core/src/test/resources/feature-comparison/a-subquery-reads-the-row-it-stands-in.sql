-- ============================================================================
-- A subquery reads the row it stands in
--
-- A subquery is written inside the scope of the query around it, so a name its
-- own FROM clause has not got is resolved against that query -- in its select
-- list exactly as in its WHERE clause. The innermost query a name belongs to
-- is the one that answers it, and a name no query in scope answers to is
-- refused with the relation that could not be reached named in the DETAIL.
--
-- Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE sqc_o (i int, j text);
CREATE TABLE sqc_x (k int);
CREATE TABLE sqc_y (i int);
CREATE TABLE sqc_n (ix int);
INSERT INTO sqc_o VALUES (1,'a'),(2,'b');
INSERT INTO sqc_x VALUES (7);
INSERT INTO sqc_y VALUES (99);
INSERT INTO sqc_n VALUES (5);

-- ============================================================================
-- A reference to the enclosing query in a subquery's select list
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- a bare name the subquery's own relations have not got is the outer query's
-- begin-expected
-- columns: j
-- row: a
-- row: b
-- end-expected
SELECT (SELECT j FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: ?column?
-- row: 8
-- row: 9
-- end-expected
SELECT (SELECT o.i + x.k FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM sqc_x x GROUP BY o.i) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: k
-- row: 7
-- row: 7
-- end-expected
SELECT (SELECT x.k FROM sqc_x x ORDER BY o.i) FROM sqc_o o;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM sqc_x x LIMIT 1) FROM sqc_o o ORDER BY 1;

-- a subquery that matches nothing still answers null, once per outer row
-- begin-expected
-- columns: i
-- row: null
-- row: null
-- end-expected
SELECT (SELECT o.i FROM sqc_x x WHERE false) FROM sqc_o o;

-- ============================================================================
-- The same reference in EXISTS, IN, ANY, ALL and ARRAY
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT o.i FROM sqc_o o WHERE EXISTS (SELECT o.i FROM sqc_x x) ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT s.i FROM sqc_o s WHERE s.i IN (SELECT s.i FROM sqc_x) ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT o.i FROM sqc_o o WHERE o.i = ANY (SELECT o.i FROM sqc_x x) ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT o.i FROM sqc_o o WHERE o.i > ALL (SELECT o.i - 1 FROM sqc_x x) ORDER BY 1;

-- begin-expected
-- columns: array
-- row: {1}
-- row: {2}
-- end-expected
SELECT ARRAY(SELECT o.i FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- ============================================================================
-- A LATERAL item reads the item to its left from its own select list
-- ============================================================================

-- begin-expected
-- columns: z
-- row: 1
-- row: 2
-- end-expected
SELECT b.z FROM sqc_o a, LATERAL (SELECT a.i AS z FROM sqc_x x) b ORDER BY 1;

-- begin-expected
-- columns: z
-- row: 1
-- row: 2
-- end-expected
SELECT b.z FROM sqc_o a JOIN LATERAL (SELECT a.i AS z FROM sqc_x x) b ON true ORDER BY 1;

-- begin-expected
-- columns: i | k
-- row: 1 | 7
-- row: 2 | 7
-- end-expected
SELECT b.i, b.k FROM sqc_o a, LATERAL (SELECT a.*, x.k FROM sqc_x x) b ORDER BY 1;

-- and it reaches that item through a query of its own
-- begin-expected
-- columns: z
-- row: 1
-- row: 2
-- end-expected
SELECT b.z FROM sqc_o a, LATERAL (SELECT (SELECT a.i) AS z) b ORDER BY 1;

-- begin-expected
-- columns: z
-- row: 1
-- row: 2
-- end-expected
SELECT b.z FROM sqc_o a, LATERAL (SELECT s.q AS z FROM (SELECT a.i AS q) s) b ORDER BY 1;

-- ============================================================================
-- Nesting, and a sub-select in the subquery's own FROM clause
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT (SELECT o.i FROM sqc_x y) FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: k
-- row: 7
-- row: 7
-- end-expected
SELECT (SELECT (SELECT x.k FROM sqc_y y) FROM sqc_x x) FROM sqc_o o;

-- begin-expected
-- columns: z
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT s.z FROM (SELECT o.i AS z FROM sqc_x x) s) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: z
-- row: 1
-- row: 2
-- end-expected
SELECT (WITH w AS (SELECT o.i AS z FROM sqc_x x) SELECT w.z FROM w) FROM sqc_o o ORDER BY 1;

-- ============================================================================
-- The innermost query a name belongs to is the one that answers it
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 99
-- row: 99
-- end-expected
SELECT (SELECT i FROM sqc_y x) FROM sqc_o o;

-- begin-expected
-- columns: i
-- row: 99
-- row: 99
-- end-expected
SELECT (SELECT x.i FROM sqc_y x) FROM sqc_o o;

-- begin-expected
-- columns: k
-- row: 7
-- row: 7
-- end-expected
SELECT (SELECT o.k FROM sqc_x o) FROM sqc_o o;

-- a qualifier the inner query answers to is not looked for outside
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column o.i does not exist
-- end-expected-error
SELECT (SELECT o.i FROM sqc_x o) FROM sqc_o o;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column o.i does not exist
-- end-expected-error
SELECT (SELECT o.i FROM sqc_n o) FROM sqc_o o;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column o.ix does not exist
-- end-expected-error
SELECT (SELECT o.ix FROM sqc_o o) FROM sqc_n o;

-- ============================================================================
-- A whole row and a qualified star read from the enclosing query
-- ============================================================================

-- begin-expected
-- columns: o
-- row: (1,a)
-- row: (2,b)
-- end-expected
SELECT (SELECT o FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: c
-- row: 99
-- row: 99
-- end-expected
SELECT (SELECT y.* FROM sqc_x x) AS c FROM sqc_o o, sqc_y y;

-- begin-expected
-- columns: exists
-- row: t
-- row: t
-- end-expected
SELECT EXISTS (SELECT o.* FROM sqc_x x) FROM sqc_o o;

-- a star of more than one column is still too wide for one value
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT (SELECT o.* FROM sqc_x x) FROM sqc_o o;

-- standing where a value is expected, the star is the outer row
-- begin-expected
-- columns: num_nonnulls
-- row: 1
-- row: 1
-- end-expected
SELECT (SELECT num_nonnulls(o.*) FROM sqc_x x) FROM sqc_o o;

-- begin-expected
-- columns: ?column?
-- row: a
-- row: b
-- end-expected
SELECT (SELECT to_jsonb(o.*)->>'j' FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT (SELECT count(o.*) FROM sqc_x x) FROM sqc_o o WHERE o.i = 1;

-- and such a subquery is labelled after the relation the star names
-- begin-expected
-- columns: k
-- row: 7
-- row: 7
-- end-expected
SELECT (SELECT a.* FROM sqc_x a, sqc_y b) FROM sqc_o o;

-- begin-expected
-- columns: i
-- row: 99
-- row: 99
-- end-expected
SELECT (SELECT b.* FROM sqc_x a, sqc_y b) FROM sqc_o o;

-- ============================================================================
-- What no query in scope answers to is still refused
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nope"
-- end-expected-error
SELECT (SELECT nope.i FROM sqc_x x) FROM sqc_o o;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "s"
-- end-expected-error
SELECT b.i FROM sqc_o a, LATERAL (SELECT s.* FROM sqc_x x) b;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column o.nosuch does not exist
-- end-expected-error
SELECT (SELECT o.nosuch FROM sqc_x x) FROM sqc_o o;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
SELECT (SELECT i FROM sqc_o a, sqc_y b) FROM sqc_o o;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
SELECT (SELECT i FROM sqc_x x) FROM sqc_o o, sqc_y y;

-- an alias renames the relation for the whole query, so its own name reaches
-- nothing
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "sqc_o"
-- end-expected-error
SELECT (SELECT sqc_o.i FROM sqc_x x) FROM sqc_o o;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "sqc_o"
-- end-expected-error
SELECT b.z FROM sqc_o o, LATERAL (SELECT sqc_o.i AS z FROM sqc_x x) b;

-- with no alias in the way the relation answers to its own name
-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT sqc_o.i FROM sqc_x x) FROM sqc_o ORDER BY 1;

-- ============================================================================
-- A relation beside this one is not in scope without LATERAL
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "o"
-- end-expected-error
SELECT s.z FROM sqc_o o, (SELECT o.i AS z FROM sqc_x x) s;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "o"
-- end-expected-error
SELECT s.z FROM sqc_o o JOIN (SELECT o.i AS z FROM sqc_x x) s ON true;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "o"
-- end-expected-error
WITH w AS (SELECT o.i AS z FROM sqc_x x) SELECT w.z FROM sqc_o o, w;

-- the same query written where the enclosing query really is above answers
-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT o.i FROM sqc_o o WHERE o.i IN (SELECT s.z FROM (SELECT o.i AS z FROM sqc_x x) s)
  ORDER BY 1;

-- ============================================================================
-- A set operation, a WITH RECURSIVE and a VALUES list inside the subquery
-- ============================================================================

-- begin-expected
-- columns: c
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM sqc_x x UNION ALL SELECT o.i FROM sqc_x y LIMIT 1) AS c
  FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: c
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM sqc_x x UNION SELECT 9 ORDER BY 1 LIMIT 1) AS c
  FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: c
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM sqc_x x INTERSECT SELECT o.i FROM sqc_x y) AS c
  FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: max
-- row: 3
-- row: 3
-- end-expected
SELECT (WITH RECURSIVE w(n) AS (SELECT o.i UNION ALL SELECT n+1 FROM w WHERE n < 3)
  SELECT max(n) FROM w) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM (VALUES (1)) v(c)) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT (SELECT o.i FROM sqc_x x LIMIT (SELECT 1)) FROM sqc_o o ORDER BY 1;

-- a subquery standing for one value is still held to one row
-- begin-expected-error
-- sqlstate: 21000
-- message-like: more than one row returned by a subquery used as an expression
-- end-expected-error
SELECT (SELECT o.i FROM generate_series(1,2) g) FROM sqc_o o;

-- ============================================================================
-- The subquery stands in every clause the query around it has
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT o.i FROM sqc_o o GROUP BY o.i HAVING (SELECT o.i FROM sqc_x x) > 0 ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 2
-- row: 1
-- end-expected
SELECT o.i FROM sqc_o o ORDER BY (SELECT o.i FROM sqc_x x) DESC;

-- begin-expected
-- columns: case
-- row: 1
-- row: 2
-- end-expected
SELECT CASE WHEN true THEN (SELECT o.i FROM sqc_x x) ELSE 0 END FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: abs
-- row: 1
-- row: 2
-- end-expected
SELECT abs((SELECT o.i FROM sqc_x x)) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: max
-- row: 2
-- end-expected
SELECT max((SELECT o.i FROM sqc_x x)) FROM sqc_o o;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT DISTINCT ON ((SELECT o.i FROM sqc_x x)) o.i FROM sqc_o o
  ORDER BY (SELECT o.i FROM sqc_x x), o.i;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT DISTINCT (SELECT o.i FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 7
-- end-expected
SELECT (SELECT o.i FROM sqc_x x) FROM sqc_o o UNION ALL SELECT k FROM sqc_x ORDER BY 1;

-- begin-expected
-- columns: ?column?
-- row: a7
-- row: b7
-- end-expected
SELECT (SELECT o.j || x.k::text FROM sqc_x x) FROM sqc_o o ORDER BY 1;

-- the row an outer join padded with nulls is the row the subquery reads
-- begin-expected
-- columns: i
-- row: null
-- row: null
-- end-expected
SELECT (SELECT y.i FROM sqc_x x) FROM sqc_o o LEFT JOIN sqc_y y ON false ORDER BY 1;

-- ============================================================================
-- A grouped query lends the subquery only what it grouped by
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "o.i" from outer query
-- end-expected-error
SELECT count(*), (SELECT o.i FROM sqc_x x) FROM sqc_o o;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "o.j" from outer query
-- end-expected-error
SELECT count(*) FROM sqc_o o GROUP BY o.i HAVING (SELECT o.j FROM sqc_x x) IS NOT NULL;

-- begin-expected
-- columns: count | i
-- row: 1 | 1
-- row: 1 | 2
-- end-expected
SELECT count(*), (SELECT o.i FROM sqc_x x) FROM sqc_o o GROUP BY o.i ORDER BY 2;

-- ============================================================================
-- The writing paths give the subquery the same scope
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
UPDATE sqc_o o SET j = j RETURNING (SELECT o.i FROM sqc_x x);

-- begin-expected
-- columns: i
-- end-expected
DELETE FROM sqc_o d WHERE d.i = 999 RETURNING (SELECT d.i FROM sqc_x x);

INSERT INTO sqc_y SELECT (SELECT o.i FROM sqc_x x) FROM sqc_o o;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 99
-- end-expected
SELECT i FROM sqc_y ORDER BY i;

UPDATE sqc_o o SET i = (SELECT o.i + 100 FROM sqc_x x);

-- begin-expected
-- columns: i
-- row: 101
-- row: 102
-- end-expected
SELECT i FROM sqc_o ORDER BY i;

CREATE VIEW sqc_v AS SELECT (SELECT o.i FROM sqc_x x) AS c FROM sqc_o o;

-- begin-expected
-- columns: c
-- row: 101
-- row: 102
-- end-expected
SELECT c FROM sqc_v ORDER BY c;

-- cleanup
DROP VIEW sqc_v;
DROP TABLE sqc_n;
DROP TABLE sqc_y;
DROP TABLE sqc_x;
DROP TABLE sqc_o;
