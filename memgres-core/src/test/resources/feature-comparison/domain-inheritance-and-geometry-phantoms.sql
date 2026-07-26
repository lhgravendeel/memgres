-- ============================================================================
-- Feature Comparison: domain constraint inheritance and geometric operators
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A domain built on another domain inherits its constraints, and PostgreSQL
-- reports the innermost one that a value violates. On the geometric side, a
-- shape only has the functions and operators PG actually defines for it: a
-- polygon has no center(), and a line contains nothing.
-- ============================================================================

DROP DOMAIN IF EXISTS dg_small CASCADE;
DROP DOMAIN IF EXISTS dg_pos CASCADE;
DROP DOMAIN IF EXISTS dg_nn CASCADE;

CREATE DOMAIN dg_pos AS int CHECK (VALUE > 0);
CREATE DOMAIN dg_small AS dg_pos CHECK (VALUE < 100);
CREATE DOMAIN dg_nn AS int NOT NULL;

-- ============================================================================
-- 1. A domain over a domain enforces both constraints
-- ============================================================================

-- The base domain's constraint is the one reported
-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "dg_pos_check"
-- end-expected-error
SELECT (-5)::dg_small;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "dg_small_check"
-- end-expected-error
SELECT 500::dg_small;

-- A value satisfying both is accepted
-- begin-expected
-- columns: a
-- row: 50
-- end-expected
SELECT 50::dg_small::text AS a;

-- ============================================================================
-- 2. A NOT NULL domain rejects null through a cast
-- ============================================================================

-- begin-expected-error
-- sqlstate: 23502
-- message-like: does not allow null values
-- end-expected-error
SELECT NULL::dg_nn;

-- ============================================================================
-- 3. center() exists only for box and circle
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function center(polygon) does not exist
-- end-expected-error
SELECT center(polygon '((0,0),(1,0),(1,1))');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function center(lseg) does not exist
-- end-expected-error
SELECT center(lseg '((0,0),(1,1))');

-- begin-expected
-- columns: a
-- row: (1,1)
-- end-expected
SELECT center(box '((0,0),(2,2))')::text AS a;

-- begin-expected
-- columns: a
-- row: (1,1)
-- end-expected
SELECT center(circle '<(1,1),5>')::text AS a;

-- ============================================================================
-- 4. @> needs a container on the left and a region on the right
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist
-- end-expected-error
SELECT (box '((0,0),(2,2))' @> lseg '((0,0),(1,1))');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist
-- end-expected-error
SELECT (line '{1,0,0}' @> point '(0,0)');

-- An open path does contain a point, and a bracketed value reads as one
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (path '[(0,0),(1,1)]' @> point '(0,0)') AS a;

-- The real containment operators still work
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (box '((0,0),(2,2))' @> point '(1,1)') AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (polygon '((0,0),(2,0),(2,2))' @> point '(1,0.5)') AS a;

-- ============================================================================
-- 5. area() exists for box, circle and path -- not for a polygon
-- ============================================================================
-- A closed path and a polygon print the same, so the declared type of the
-- argument is what decides which overload applies.

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function area(polygon) does not exist
-- end-expected-error
SELECT area(polygon '((0,0),(4,0),(4,3),(0,3))');

-- begin-expected
-- columns: a
-- row: 12
-- end-expected
SELECT area(path '((0,0),(4,0),(4,3),(0,3))') AS a;

-- begin-expected
-- columns: a
-- row: 12
-- end-expected
SELECT area(box '((0,0),(4,3))') AS a;

-- begin-expected
-- columns: a
-- row: 12.566370614359172
-- end-expected
SELECT area(circle '<(0,0),2>') AS a;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function area(lseg) does not exist
-- end-expected-error
SELECT area(lseg '[(0,0),(1,1)]');

-- A column carries its declared type too
DROP TABLE IF EXISTS dg_g CASCADE;
CREATE TABLE dg_g (id int PRIMARY KEY, pg polygon, pa path, bx box);
INSERT INTO dg_g VALUES (1, '((0,0),(4,0),(4,3),(0,3))', '((0,0),(4,0),(4,3),(0,3))', '((0,0),(4,3))');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function area(polygon) does not exist
-- end-expected-error
SELECT area(pg) FROM dg_g;

-- begin-expected
-- columns: a
-- row: 12
-- end-expected
SELECT area(pa) AS a FROM dg_g;

-- begin-expected
-- columns: a
-- row: 12
-- end-expected
SELECT area(bx) AS a FROM dg_g;

DROP TABLE dg_g;
DROP DOMAIN dg_small;
DROP DOMAIN dg_pos;
DROP DOMAIN dg_nn;
