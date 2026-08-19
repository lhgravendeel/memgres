-- ============================================================================
-- An aggregate or window call is found wherever it is written
--
-- Whether a query is grouped, and which calls the window pass has to answer for, is
-- decided by looking through an expression for the calls written inside it. Asked
-- by naming the node types one at a time, that question is answered only for the
-- shapes somebody remembered to list. A call written under BETWEEN, inside ARRAY[]
-- or ROW(), in an IN list, under COLLATE, or subscripted, was not found: the query
-- ran ungrouped over rows PostgreSQL folds into one, or took the plain path where
-- such a call has no value, and the row answered NULL -- or, in HAVING, no row at
-- all.
--
-- A call written inside a sub-select belongs to that sub-select, and is not what
-- groups the query it is written in.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE agc_t (g int, v int, s text);
INSERT INTO agc_t VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 30, 'c');

-- ============================================================================
-- An aggregate is an aggregate wherever it is written
-- ============================================================================

-- One row per group is what the aggregate asks for, and the container it is
-- written inside does not change that.
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT max(v) BETWEEN 1 AND 100 FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 15 BETWEEN min(v) AND max(v) FROM agc_t;

-- begin-expected
-- columns: array
-- row: {3}
-- end-expected
SELECT ARRAY[count(*)] FROM agc_t;

-- begin-expected
-- columns: row
-- row: (10,30)
-- end-expected
SELECT ROW(min(v), max(v)) FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 3 IN (count(*), 99) FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT count(*) IN (1, 2, 3) FROM agc_t;

-- begin-expected
-- columns: max
-- row: c
-- end-expected
SELECT max(s) COLLATE "C" FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT (max(v) > 5) IS TRUE FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT (min(v) IS NULL) IS NOT FALSE FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT max(s) LIKE 'c%' FROM agc_t;

-- A subscript reads into the array the aggregates built, so the array is built
-- once over the group and the subscript reads that one.
-- begin-expected
-- columns: array
-- row: 10
-- end-expected
SELECT (ARRAY[min(v), max(v)])[1] FROM agc_t;

-- begin-expected
-- columns: array
-- row: 30
-- end-expected
SELECT (ARRAY[min(v), max(v)])[2] FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT ARRAY[min(v), max(v)] @> ARRAY[10] FROM agc_t;

-- An ordered-set aggregate is one too.
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) BETWEEN 1 AND 100 FROM agc_t;

-- begin-expected
-- columns: array
-- row: {20}
-- end-expected
SELECT ARRAY[percentile_disc(0.5) WITHIN GROUP (ORDER BY v)] FROM agc_t;

-- and what a subquery answers is compared against the folded value
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT count(*) IN (SELECT 3) FROM agc_t;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT count(*) = ANY (ARRAY[1,3]) FROM agc_t;

-- ============================================================================
-- What HAVING says is read over the group as well
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM agc_t GROUP BY g HAVING count(*) BETWEEN 1 AND 1;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM agc_t GROUP BY g HAVING 1 IN (count(*), 5);

-- begin-expected
-- columns: g | case
-- row: 1 | many
-- row: 2 | one
-- end-expected
SELECT g, CASE WHEN count(*) BETWEEN 1 AND 1 THEN 'one' ELSE 'many' END FROM agc_t GROUP BY g ORDER BY g;

-- ============================================================================
-- A window call answers wherever it is written
-- ============================================================================

-- begin-expected
-- columns: ?column?
-- row: t
-- row: t
-- row: t
-- end-expected
SELECT rank() OVER () BETWEEN 1 AND 5 FROM agc_t;

-- begin-expected
-- columns: array
-- row: {1}
-- row: {2}
-- row: {3}
-- end-expected
SELECT ARRAY[rank() OVER (ORDER BY v)] FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: ?column?
-- row: f
-- row: f
-- row: t
-- end-expected
SELECT 1 IN (rank() OVER (ORDER BY v)) FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: row
-- row: (1)
-- row: (2)
-- row: (3)
-- end-expected
SELECT ROW(rank() OVER (ORDER BY v)) FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: ?column?
-- row: f
-- row: f
-- row: f
-- end-expected
SELECT v BETWEEN 1 AND rank() OVER () FROM agc_t;

-- begin-expected
-- columns: rank | array
-- row: 1 | {1}
-- row: 2 | {2}
-- row: 3 | {3}
-- end-expected
SELECT rank() OVER w, ARRAY[rank() OVER w] FROM agc_t WINDOW w AS (ORDER BY v) ORDER BY 1;

-- The shapes that already answered still answer.
-- begin-expected
-- columns: ?column?
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT rank() OVER (ORDER BY v) + 1 FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: ?column?
-- row: -3
-- row: -2
-- row: -1
-- end-expected
SELECT -rank() OVER (ORDER BY v) FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: rank
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT (rank() OVER (ORDER BY v))::text FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: ?column?
-- row: f
-- row: f
-- row: f
-- end-expected
SELECT rank() OVER (ORDER BY v) IS NULL FROM agc_t;

-- begin-expected
-- columns: coalesce
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT coalesce(rank() OVER (ORDER BY v), 0) FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: case
-- row: x
-- row: y
-- row: y
-- end-expected
SELECT CASE WHEN rank() OVER (ORDER BY v) = 1 THEN 'x' ELSE 'y' END FROM agc_t ORDER BY 1;

-- The window value keeps the type its own expression has.
-- begin-expected
-- columns: pg_typeof
-- row: bigint
-- row: bigint
-- row: bigint
-- end-expected
SELECT pg_typeof(sum(v) OVER ()) FROM agc_t;

-- begin-expected
-- columns: sum
-- row: 10
-- row: 30
-- row: 50
-- end-expected
SELECT sum(v) OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM agc_t;

-- ============================================================================
-- A call written inside a sub-select belongs to that sub-select
-- ============================================================================

-- The outer query is not grouped by it, so it answers one row per outer row.
-- begin-expected
-- columns: count
-- row: 2
-- row: 2
-- row: 2
-- end-expected
SELECT (SELECT count(*) FROM agc_t WHERE v BETWEEN 1 AND 25) FROM agc_t ORDER BY 1;

-- begin-expected
-- columns: g | v
-- row: 2 | 30
-- end-expected
SELECT g, v FROM agc_t WHERE v IN (SELECT max(v) FROM agc_t) ORDER BY g;

-- and an aggregate is still refused where one may not be written
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT g, v FROM agc_t WHERE v BETWEEN 1 AND count(*);

-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT ARRAY[v] FROM agc_t GROUP BY g;

-- teardown
DROP TABLE agc_t;
