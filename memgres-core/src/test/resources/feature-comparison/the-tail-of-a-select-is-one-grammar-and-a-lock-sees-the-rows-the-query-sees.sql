-- ============================================================================
-- The tail of a SELECT is one grammar, and a lock sees the rows the query sees
--
-- What follows a query's ORDER BY is not a run of independent optional words. PostgreSQL writes the
-- row count and the starting point as a single clause -- either order, but always together -- and
-- the locking clause sits before that clause or after it, never through the middle of it. So
-- OFFSET 1 LIMIT 2 FOR UPDATE and FOR UPDATE LIMIT 2 OFFSET 1 are both queries, while
-- LIMIT 2 FOR UPDATE OFFSET 1 splits a clause in half and is not one.
--
-- The locking clause is spelled out in full or not written at all. FOR NO is not FOR NO KEY UPDATE
-- waiting to be finished, and FOR UPDATE SKIP does not skip anything: each is a statement that ran
-- out before it said what it meant.
--
-- A lock is taken on rows a scan produced. A set-returning call in the select list makes rows no
-- scan produced, so the two are refused together rather than locking some rows once and others
-- several times -- and ORDER BY counts as part of that list, because that is where PostgreSQL puts
-- the expressions it sorts by.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE s9_a (id int, v text);
INSERT INTO s9_a VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four');
CREATE TABLE s9_part (id int, v text) PARTITION BY RANGE (id);
CREATE TABLE s9_part1 PARTITION OF s9_part FOR VALUES FROM (1) TO (10);
INSERT INTO s9_part VALUES (1, 'a'), (2, 'b');
CREATE TABLE s9_parent (id int);
CREATE TABLE s9_child () INHERITS (s9_parent);
INSERT INTO s9_parent VALUES (1);
INSERT INTO s9_child VALUES (2);

-- ============================================================================
-- The row count and the starting point are one clause, in either order
-- ============================================================================
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id LIMIT 2 OFFSET 1;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id OFFSET 1 LIMIT 2;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id OFFSET 1 FETCH FIRST 2 ROWS ONLY;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id FETCH FIRST 2 ROWS ONLY OFFSET 1;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
-- begin-expected
-- columns: id
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id LIMIT ALL OFFSET 2;
-- begin-expected
-- columns: id
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id OFFSET 2 LIMIT ALL;

-- ============================================================================
-- That clause comes before the locking clause or after it
-- ============================================================================
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id LIMIT 2 OFFSET 1 FOR UPDATE;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id OFFSET 1 LIMIT 2 FOR UPDATE;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE LIMIT 2 OFFSET 1;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE OFFSET 1 LIMIT 2;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE FETCH FIRST 2 ROWS ONLY;
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR SHARE LIMIT 1;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM s9_a ORDER BY id LIMIT 1 FOR SHARE;

-- It is never written through the middle of that clause.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a ORDER BY id LIMIT 2 FOR UPDATE OFFSET 1;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a ORDER BY id OFFSET 1 FOR UPDATE LIMIT 2;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a ORDER BY id FETCH FIRST 2 ROWS ONLY FOR UPDATE OFFSET 1;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a ORDER BY id FOR UPDATE LIMIT 1 FOR SHARE;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a ORDER BY id LIMIT 1 OFFSET 1 OFFSET 1;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a ORDER BY id LIMIT 1 LIMIT 1;

-- ============================================================================
-- A locking clause names its strength in full
-- ============================================================================
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR NO KEY UPDATE;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR SHARE;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR KEY SHARE;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE OF s9_a;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE NOWAIT;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE SKIP LOCKED;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE OF s9_a NOWAIT;

-- A strength that stops part-way through is not a strength.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR NO;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR NO KEY;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR KEY;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR NO UPDATE;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR KEY UPDATE;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR NO KEY SHARE;

-- Neither is an option that stops part-way through.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR UPDATE SKIP;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR UPDATE LOCKED;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR UPDATE OF;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR UPDATE NOWAIT SKIP LOCKED;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT id FROM s9_a FOR UPDATE SKIP LOCKED NOWAIT;

-- ============================================================================
-- A lock and a set-returning call in the select list are refused together
-- ============================================================================
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT generate_series(1, 2) FROM s9_a FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT generate_series(1, 2) FROM s9_a FOR NO KEY UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT generate_series(1, 2) FROM s9_a FOR SHARE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT generate_series(1, 2) FROM s9_a FOR KEY SHARE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT id, generate_series(1, 2) FROM s9_a FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT generate_series(1, 2) + 1 FROM s9_a FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT unnest(ARRAY[1, 2]) FROM s9_a FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT json_array_elements('[1,2]'::json) FROM s9_a FOR SHARE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT generate_series(1, 2) FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT generate_series(1, 2) FROM s9_a FOR UPDATE OF s9_a;

-- ORDER BY is part of that list, and a subquery in FROM carries its own.
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT id FROM s9_a ORDER BY generate_series(1, 2) FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT * FROM (SELECT generate_series(1, 2) g FROM s9_a) s FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT * FROM (SELECT generate_series(1, 2) g FROM s9_a) s FOR KEY SHARE;

-- A call that produces another query's rows produces them there, not here.
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM s9_a WHERE id IN (SELECT generate_series(1, 2)) ORDER BY id FOR UPDATE;
-- begin-expected
-- columns: id
-- row: 1
-- row: 1
-- row: 2
-- row: 2
-- row: 3
-- row: 3
-- row: 4
-- row: 4
-- end-expected
SELECT id FROM s9_a, generate_series(1, 2) g ORDER BY id, g FOR UPDATE OF s9_a;
-- begin-expected
-- columns: generate_series
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- row: 2
-- row: 2
-- row: 2
-- row: 2
-- end-expected
SELECT generate_series(1, 2) FROM s9_a ORDER BY 1;

-- ============================================================================
-- A lock reads the rows the query reads
-- ============================================================================
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM s9_a ORDER BY id FOR UPDATE SKIP LOCKED;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM s9_part ORDER BY id FOR UPDATE SKIP LOCKED;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM s9_parent ORDER BY id FOR UPDATE SKIP LOCKED;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM s9_part ORDER BY id FOR UPDATE NOWAIT;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM s9_parent ORDER BY id FOR NO KEY UPDATE;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM s9_part ORDER BY id LIMIT 1 FOR UPDATE;
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM s9_parent ORDER BY id OFFSET 1 FOR SHARE;

-- ============================================================================
-- What collapses rows cannot be locked
-- ============================================================================
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT DISTINCT id FROM s9_a FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT count(*) FROM s9_a FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT id FROM s9_a GROUP BY id FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT id FROM s9_a GROUP BY id HAVING count(*) > 0 FOR SHARE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT row_number() OVER (ORDER BY id) FROM s9_a FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT id FROM (SELECT DISTINCT id FROM s9_a) s FOR UPDATE;

-- teardown
DROP TABLE s9_child;
DROP TABLE s9_parent;
DROP TABLE s9_part;
DROP TABLE s9_a;
