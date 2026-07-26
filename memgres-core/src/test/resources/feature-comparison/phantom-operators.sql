-- ============================================================================
-- Feature Comparison: operators PostgreSQL does not define
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG's operator set is narrower than the values suggest: a point compares with
-- ~= and never =, an lseg has no interior so it contains nothing, and money
-- compares only with money. A geometric value is stored as text, so only the
-- declared type of the operand can decide -- a value-level rule would break
-- ordinary string comparison.
-- ============================================================================

DROP TABLE IF EXISTS pho CASCADE;
CREATE TABLE pho (id int PRIMARY KEY, p point, l lseg);
INSERT INTO pho VALUES (1,'(1,2)','[(0,0),(1,1)]');

-- ============================================================================
-- 1. A point has no equality operator
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: point = point
-- end-expected-error
SELECT (point '(1,2)' = point '(1,2)');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: point = point
-- end-expected-error
SELECT (p = p) FROM pho;

-- ~= is the one PG defines
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (point '(1,2)' ~= point '(1,2)') AS a;

-- ============================================================================
-- 2. An lseg contains nothing
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: lseg @>
-- end-expected-error
SELECT (lseg '[(0,0),(1,1)]' @> point '(0,0)');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: lseg @>
-- end-expected-error
SELECT (l @> point '(0,0)') FROM pho;

-- An open path, whose text is identical, does contain a point
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (path '[(0,0),(1,1)]' @> point '(0,0)') AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (box '((0,0),(2,2))' @> point '(1,1)') AS a;

-- ============================================================================
-- 3. money compares only with money
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: money = numeric
-- end-expected-error
SELECT ('1'::money = 1::numeric);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: money = integer
-- end-expected-error
SELECT ('1'::money = 1::int);

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('1'::money = '1'::money) AS a;

-- ============================================================================
-- 4. Operators PG does define are untouched
-- ============================================================================

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (box '((0,0),(2,2))' = box '((0,0),(2,2))') AS a;

-- Ordinary text comparison must not be caught by the point rule
-- begin-expected
-- columns: a | b
-- row: true, true
-- end-expected
SELECT ('(1,2)'::text = '(1,2)'::text) AS a, ('abc' = 'abc') AS b;

DROP TABLE pho;
