-- Where a window function, and the parts of a window specification, may appear:
-- 1. DISTINCT and an aggregate's own ORDER BY inside a window aggregate
-- 2. FILTER -- what its condition may contain, and the FILTER that must keep working
-- 3. an aggregate may not contain a window function
-- 4. a frame offset is one size for the whole window: no column, aggregate or window function
-- 5. a frame offset is otherwise an ordinary expression and must be parsed as one
-- 6. LIMIT and OFFSET are read before any row is framed
-- 7. ORDER BY over a window function orders by the value it computes
-- 8. the placements already refused (GROUP BY, HAVING, WHERE) stay refused

-- setup
CREATE TABLE wpl_t (i int, g text, v int);
INSERT INTO wpl_t VALUES (1,'a',10),(2,'a',20),(3,'a',20),(4,'b',30),(5,'b',40);

-- ============================================================================
-- 1. DISTINCT and aggregate ORDER BY inside a window aggregate
-- ============================================================================

-- stmt 1: DISTINCT is not implemented for window aggregates
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: DISTINCT is not implemented for window functions
-- end-expected-error
SELECT count(DISTINCT v) OVER (PARTITION BY g) FROM wpl_t;

-- stmt 2: the same for every aggregate, and for a named window
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: DISTINCT is not implemented for window functions
-- end-expected-error
SELECT sum(DISTINCT v) OVER w FROM wpl_t WINDOW w AS (PARTITION BY g);

-- stmt 3: refused before the rows are read, so an empty table refuses it too
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: DISTINCT is not implemented for window functions
-- end-expected-error
SELECT array_agg(DISTINCT v) OVER (ORDER BY i) FROM wpl_t WHERE 1 = 0;

-- stmt 4: an aggregate's own ORDER BY is not implemented for window functions either
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate ORDER BY is not implemented for window functions
-- end-expected-error
SELECT array_agg(v ORDER BY v) OVER () FROM wpl_t;

-- stmt 5: an ordered-set aggregate has no window form at all
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: OVER is not supported for ordered-set aggregate percentile_cont
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) OVER () FROM wpl_t;

-- stmt 6: DISTINCT and ORDER BY in a plain aggregate are unaffected
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(DISTINCT v) FROM wpl_t;

-- begin-expected
-- columns: array_agg
-- row: {10,20,20,30,40}
-- end-expected
SELECT array_agg(v ORDER BY v) FROM wpl_t;

-- begin-expected
-- columns: g | count
-- row: a, 2
-- row: b, 2
-- end-expected
SELECT g, count(DISTINCT v) FROM wpl_t GROUP BY g ORDER BY g;

-- ============================================================================
-- 2. FILTER
-- ============================================================================

-- stmt 7: a FILTER condition is tested per row, so it cannot contain an aggregate
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in FILTER
-- end-expected-error
SELECT sum(v) FILTER (WHERE sum(v) > 15) OVER () FROM wpl_t;

-- stmt 8: nor in a plain aggregate's FILTER
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in FILTER
-- end-expected-error
SELECT g, sum(v) FILTER (WHERE count(*) > 1) FROM wpl_t GROUP BY g;

-- stmt 9: nor a window function, which needs a frame that does not exist yet
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in FILTER
-- end-expected-error
SELECT sum(v) FILTER (WHERE rank() OVER (ORDER BY i) > 1) OVER () FROM wpl_t;

-- stmt 10: including on a plain aggregate
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in FILTER
-- end-expected-error
SELECT array_agg(v) FILTER (WHERE rank() OVER () > 1) FROM wpl_t;

-- stmt 11: FILTER on a window aggregate still filters the frame
-- begin-expected
-- columns: i | sum
-- row: 1, NULL
-- row: 2, 20
-- row: 3, 40
-- row: 4, 70
-- row: 5, 110
-- end-expected
SELECT i, sum(v) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wpl_t ORDER BY i;

-- begin-expected
-- columns: i | count
-- row: 1, 1
-- row: 2, 2
-- row: 3, 3
-- row: 4, 3
-- row: 5, 3
-- end-expected
SELECT i, count(*) FILTER (WHERE g = 'a') OVER (ORDER BY i) FROM wpl_t ORDER BY i;

-- stmt 12: a sub-select in the condition is a constant, not an aggregate
-- begin-expected
-- columns: i | sum
-- row: 1, 110
-- row: 2, 110
-- row: 3, 110
-- row: 4, 110
-- row: 5, 110
-- end-expected
SELECT i, sum(v) FILTER (WHERE v > (SELECT 15)) OVER () FROM wpl_t ORDER BY i;

-- ============================================================================
-- 3. An aggregate may not contain a window function
-- ============================================================================

-- stmt 13: the window function would have to be numbered before the group exists
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot contain window function calls
-- end-expected-error
SELECT sum(rank() OVER (ORDER BY v)) FROM wpl_t;

-- stmt 14: buried in an expression under the aggregate, and with a GROUP BY
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot contain window function calls
-- end-expected-error
SELECT sum(1 + rank() OVER (ORDER BY v)) FROM wpl_t;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot contain window function calls
-- end-expected-error
SELECT g, sum(rank() OVER (ORDER BY v)) FROM wpl_t GROUP BY g;

-- stmt 15: and in the aggregate's own ORDER BY
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot contain window function calls
-- end-expected-error
SELECT sum(v ORDER BY rank() OVER ()) FROM wpl_t;

-- stmt 16: an aggregate as a window function may not contain one either
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window function calls cannot be nested
-- end-expected-error
SELECT sum(rank() OVER (ORDER BY v)) OVER () FROM wpl_t;

-- stmt 17: an ordinary function around a window function is fine
-- begin-expected
-- columns: abs
-- row: 1
-- row: 2
-- row: 2
-- row: 4
-- row: 5
-- end-expected
SELECT abs(rank() OVER (ORDER BY v)) FROM wpl_t ORDER BY 1;

-- begin-expected
-- columns: rank
-- row: 1
-- row: 2
-- row: 2
-- row: 4
-- row: 5
-- end-expected
SELECT rank() OVER (ORDER BY abs(v)) FROM wpl_t ORDER BY 1;

-- ============================================================================
-- 4. What a frame offset may not contain
-- ============================================================================

-- stmt 18: a window function in a frame offset
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN rank() OVER () PRECEDING AND CURRENT ROW) FROM wpl_t;

-- stmt 19: an aggregate in a frame offset, named by the frame's own mode
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in window ROWS
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN count(*) PRECEDING AND CURRENT ROW) FROM wpl_t;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in window RANGE
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN count(*) PRECEDING AND CURRENT ROW) FROM wpl_t;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in window GROUPS
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN count(*) PRECEDING AND CURRENT ROW) FROM wpl_t;

-- stmt 20: the ending offset is checked as well as the starting one
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in window ROWS
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND count(*) FOLLOWING) FROM wpl_t;

-- stmt 21: a column reference is a per-row value, which a frame size may not be
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: argument of ROWS must not contain variables
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN v PRECEDING AND CURRENT ROW) FROM wpl_t;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: argument of RANGE must not contain variables
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN v PRECEDING AND CURRENT ROW) FROM wpl_t;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: argument of GROUPS must not contain variables
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN v PRECEDING AND CURRENT ROW) FROM wpl_t;

-- stmt 22: a WINDOW entry nothing references is checked the same way
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in window ROWS
-- end-expected-error
SELECT 1 FROM wpl_t WINDOW w AS (ORDER BY i ROWS BETWEEN count(*) PRECEDING AND CURRENT ROW);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: argument of ROWS must not contain variables
-- end-expected-error
SELECT 1 FROM wpl_t WINDOW w AS (ORDER BY i ROWS BETWEEN v PRECEDING AND CURRENT ROW);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT 1 FROM wpl_t WINDOW w AS (ORDER BY i ROWS BETWEEN rank() OVER () PRECEDING AND CURRENT ROW);

-- ============================================================================
-- 5. A frame offset is otherwise an ordinary expression
-- ============================================================================

-- stmt 23: arithmetic in a frame offset
-- begin-expected
-- columns: i | sum
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 70
-- row: 5, 90
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1+1 PRECEDING AND CURRENT ROW) FROM wpl_t ORDER BY i;

-- begin-expected
-- columns: i | sum
-- row: 1, 30
-- row: 2, 50
-- row: 3, 80
-- row: 4, 110
-- row: 5, 90
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 2*1 PRECEDING AND 3-2 FOLLOWING) FROM wpl_t ORDER BY i;

-- stmt 24: a leading sign is part of the expression, not a separate rule
-- begin-expected
-- columns: i | sum
-- row: 1, 10
-- row: 2, 30
-- row: 3, 40
-- row: 4, 50
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN -1+2 PRECEDING AND CURRENT ROW) FROM wpl_t ORDER BY i;

-- stmt 25: a function call, and a RANGE offset computed the same way
-- begin-expected
-- columns: i | sum
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 70
-- row: 5, 90
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN abs(-2) PRECEDING AND CURRENT ROW) FROM wpl_t ORDER BY i;

-- begin-expected
-- columns: i | sum
-- row: 1, 10
-- row: 2, 50
-- row: 3, 50
-- row: 4, 70
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v RANGE BETWEEN 5+5 PRECEDING AND CURRENT ROW) FROM wpl_t ORDER BY i;

-- stmt 26: a sub-select reads its own rows, so it is a constant here
-- begin-expected
-- columns: i | sum
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 80
-- row: 5, 120
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN (SELECT max(v) FROM wpl_t)/10 PRECEDING AND CURRENT ROW)
FROM wpl_t ORDER BY i;

-- stmt 27: the offset errors that were already reported still are
-- begin-expected-error
-- sqlstate: 22013
-- message-like: frame starting offset must not be negative
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wpl_t;

-- begin-expected-error
-- sqlstate: 22004
-- message-like: frame starting offset must not be null
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wpl_t;

-- ============================================================================
-- 6. LIMIT and OFFSET
-- ============================================================================

-- stmt 28: LIMIT is read once for the query, before any row is framed
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in LIMIT
-- end-expected-error
SELECT i FROM wpl_t LIMIT rank() OVER ();

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in OFFSET
-- end-expected-error
SELECT i FROM wpl_t OFFSET rank() OVER ();

-- ============================================================================
-- 7. ORDER BY over a window function
-- ============================================================================

-- stmt 29: ordering by a window function that is not in the select list
-- begin-expected
-- columns: i | v
-- row: 5, 40
-- row: 4, 30
-- row: 2, 20
-- row: 3, 20
-- row: 1, 10
-- end-expected
SELECT i, v FROM wpl_t ORDER BY row_number() OVER (ORDER BY v DESC);

-- stmt 30: with a direction of its own, and inside an expression
-- begin-expected
-- columns: i
-- row: 5
-- row: 4
-- row: 2
-- row: 3
-- row: 1
-- end-expected
SELECT i FROM wpl_t ORDER BY rank() OVER (ORDER BY v) DESC, i;

-- begin-expected
-- columns: i
-- row: 5
-- row: 4
-- row: 2
-- row: 3
-- row: 1
-- end-expected
SELECT i FROM wpl_t ORDER BY 1 + rank() OVER (ORDER BY v DESC);

-- stmt 31: an aggregate as the window function
-- begin-expected
-- columns: i
-- row: 5
-- row: 4
-- row: 3
-- row: 2
-- row: 1
-- end-expected
SELECT i FROM wpl_t ORDER BY sum(v) OVER (ORDER BY i) DESC;

-- stmt 32: one window function selected, another ordered by
-- begin-expected
-- columns: i | rank
-- row: 5, 5
-- row: 4, 4
-- row: 3, 2
-- row: 2, 2
-- row: 1, 1
-- end-expected
SELECT i, rank() OVER (ORDER BY v) FROM wpl_t ORDER BY row_number() OVER (ORDER BY i DESC);

-- stmt 33: WHERE, LIMIT, OFFSET and a star target keep their meaning around it
-- begin-expected
-- columns: i
-- row: 4
-- row: 2
-- end-expected
SELECT i FROM wpl_t ORDER BY rank() OVER (ORDER BY v DESC) LIMIT 2 OFFSET 1;

-- begin-expected
-- columns: i
-- row: 2
-- row: 3
-- row: 1
-- end-expected
SELECT i FROM wpl_t WHERE g = 'a' ORDER BY rank() OVER (ORDER BY v DESC);

-- begin-expected
-- columns: i | g | v
-- row: 5, b, 40
-- row: 4, b, 30
-- row: 2, a, 20
-- row: 3, a, 20
-- row: 1, a, 10
-- end-expected
SELECT * FROM wpl_t ORDER BY rank() OVER (ORDER BY v DESC);

-- stmt 34: SELECT DISTINCT still requires the ordering expression in the select list
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- end-expected-error
SELECT DISTINCT v FROM wpl_t ORDER BY rank() OVER (ORDER BY v DESC);

-- stmt 35: DISTINCT ON keeps its first row per group
-- begin-expected
-- columns: i
-- row: 1
-- row: 4
-- end-expected
SELECT DISTINCT ON (g) i FROM wpl_t ORDER BY g, rank() OVER (ORDER BY v);

-- ============================================================================
-- 8. The placements already refused, and the ones that must keep working
-- ============================================================================

-- stmt 36: GROUP BY, HAVING and WHERE
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in GROUP BY
-- end-expected-error
SELECT g FROM wpl_t GROUP BY rank() OVER (ORDER BY v);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in HAVING
-- end-expected-error
SELECT g FROM wpl_t GROUP BY g HAVING rank() OVER (ORDER BY g) = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT i FROM wpl_t WHERE rank() OVER (ORDER BY v) = 1;

-- stmt 37: a window function in a subquery, filtered by its result
-- begin-expected
-- columns: i | rn
-- row: 1, 1
-- row: 2, 2
-- row: 3, 2
-- row: 4, 4
-- row: 5, 5
-- end-expected
SELECT * FROM (SELECT i, rank() OVER (ORDER BY v) rn FROM wpl_t) s WHERE s.rn >= 1 ORDER BY i;

-- stmt 38: through a CTE, a join and a LATERAL
-- begin-expected
-- columns: i | rn
-- row: 1, 1
-- row: 2, 2
-- row: 3, 2
-- row: 4, 4
-- row: 5, 5
-- end-expected
WITH c AS (SELECT i, rank() OVER (ORDER BY v) rn FROM wpl_t) SELECT * FROM c ORDER BY i;

-- begin-expected
-- columns: i | rn
-- row: 1, 1
-- row: 2, 2
-- row: 3, 2
-- row: 4, 4
-- row: 5, 5
-- end-expected
SELECT a.i, b.rn FROM wpl_t a JOIN (SELECT i, rank() OVER (ORDER BY v) rn FROM wpl_t) b
ON a.i = b.i ORDER BY a.i;

-- begin-expected
-- columns: i | rn
-- row: 1, 1
-- row: 2, 1
-- row: 3, 1
-- row: 4, 1
-- row: 5, 1
-- end-expected
SELECT i, x.rn FROM wpl_t t, LATERAL (SELECT rank() OVER (ORDER BY t.v) rn) x ORDER BY i;

-- stmt 39: ordinary frames and window functions are unchanged
-- begin-expected
-- columns: i | sum
-- row: 1, 10
-- row: 2, 30
-- row: 3, 40
-- row: 4, 50
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wpl_t ORDER BY i;

-- begin-expected
-- columns: i | ntile
-- row: 1, 1
-- row: 2, 1
-- row: 3, 1
-- row: 4, 2
-- row: 5, 2
-- end-expected
SELECT i, ntile(2) OVER (ORDER BY i) FROM wpl_t ORDER BY i;

-- cleanup
DROP TABLE wpl_t;
