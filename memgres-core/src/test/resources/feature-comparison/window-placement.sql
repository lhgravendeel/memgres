-- Where a window function, and the parts of a window specification, may appear:
-- 1. DISTINCT and an aggregate's own ORDER BY inside a window aggregate
-- 2. FILTER -- what its condition may contain, and the FILTER that must keep working
-- 3. an aggregate may not contain a window function
-- 4. a frame offset is one size for the whole window: no column, aggregate or window function
-- 5. a frame offset is otherwise an ordinary expression and must be parsed as one
-- 6. LIMIT and OFFSET are read before any row is framed
-- 7. ORDER BY over a window function orders by the value it computes
-- 8. the placements already refused (GROUP BY, HAVING, WHERE) stay refused
-- 9. a window function over a grouped result: an aggregate under it is that result's value
-- 10. what the window reads -- arguments, PARTITION BY, ORDER BY, FILTER -- is the grouped row
-- 11. grouping sets, HAVING, DISTINCT and LIMIT around a window over grouped rows
-- 12. a nested aggregate with no OVER is still refused, and the grouping rules still hold

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

-- ============================================================================
-- 9. an aggregate under a window function, over a grouped result
-- ============================================================================

-- stmt 40: the window runs on the grouped rows, so sum(v) is an ordinary value to it
-- begin-expected
-- columns: sum
-- row: 120
-- row: 120
-- end-expected
SELECT sum(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 2
-- row: 2
-- end-expected
SELECT count(count(*)) OVER () FROM wpl_t GROUP BY g ORDER BY 1;

-- begin-expected
-- columns: g | max | min
-- row: a, 70, 50
-- row: b, 70, 50
-- end-expected
SELECT g, max(sum(v)) OVER (), min(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | min | max
-- row: a, 10, 30
-- row: b, 30, 30
-- end-expected
SELECT g, min(v), max(min(v)) OVER () FROM wpl_t GROUP BY g ORDER BY g;

-- stmt 41: one group, written without GROUP BY -- and none at all
-- begin-expected
-- columns: sum
-- row: 120
-- end-expected
SELECT sum(sum(v)) OVER () FROM wpl_t;

-- begin-expected
-- columns: sum
-- row: NULL
-- end-expected
SELECT sum(sum(v)) OVER () FROM wpl_t WHERE 1 = 0;

-- stmt 42: an aggregate only inside the OVER clause groups the query just the same
-- begin-expected
-- columns: rank
-- row: 1
-- end-expected
SELECT rank() OVER (ORDER BY sum(v)) FROM wpl_t;

-- ============================================================================
-- 10. what the window reads is the grouped row
-- ============================================================================

-- stmt 43: ORDER BY inside the window reads the group's aggregate, not nothing
-- begin-expected
-- columns: g | sum | rank
-- row: a, 50, 1
-- row: b, 70, 2
-- end-expected
SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | rank
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT g, rank() OVER (ORDER BY sum(v) DESC) FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: i
-- row: 5
-- row: 4
-- row: 3
-- row: 2
-- row: 1
-- end-expected
SELECT i FROM wpl_t GROUP BY i ORDER BY rank() OVER (ORDER BY i DESC);

-- stmt 44: PARTITION BY reads it too
-- begin-expected
-- columns: g | sum | rank
-- row: a, 50, 1
-- row: b, 70, 1
-- end-expected
SELECT g, sum(v), rank() OVER (PARTITION BY sum(v) ORDER BY g) FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | count | sum
-- row: a, 3, 50
-- row: b, 2, 70
-- end-expected
SELECT g, count(*), sum(sum(v)) OVER (PARTITION BY count(*)) FROM wpl_t GROUP BY g ORDER BY g;

-- stmt 45: a grouping expression the select list does not carry
-- begin-expected
-- columns: rank
-- row: 1
-- row: 2
-- end-expected
SELECT rank() OVER (ORDER BY g DESC) FROM wpl_t GROUP BY g ORDER BY 1;

-- begin-expected
-- columns: d | count | rank
-- row: 1, 1, 2
-- row: 2, 2, 1
-- row: 3, 1, 3
-- row: 4, 1, 4
-- end-expected
SELECT v/10 d, count(*), rank() OVER (ORDER BY count(*) DESC, v/10) FROM wpl_t
GROUP BY v/10 ORDER BY 1;

-- stmt 46: a frame over grouped rows frames the groups
-- begin-expected
-- columns: g | sum | sum
-- row: a, 50, 50
-- row: b, 70, 120
-- end-expected
SELECT g, sum(v), sum(sum(v)) OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | sum
-- row: a, 50
-- row: b, 120
-- end-expected
SELECT g, sum(sum(v)) OVER (ORDER BY sum(v) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM wpl_t GROUP BY g ORDER BY g;

-- the default frame ends at the last peer, and the peers are grouped rows
-- begin-expected
-- columns: g | sum | sum
-- row: a, 10, 10
-- row: a, 20, 50
-- row: a, 20, 50
-- row: b, 30, 80
-- row: b, 40, 120
-- end-expected
SELECT g, sum(v), sum(sum(v)) OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g, i ORDER BY g, i;

-- stmt 47: the functions that read one row of the frame read a grouped row
-- begin-expected
-- columns: g | sum | lag
-- row: a, 50, NULL
-- row: b, 70, 50
-- end-expected
SELECT g, sum(v), lag(sum(v)) OVER (ORDER BY g) FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | sum | nth_value
-- row: a, 50, 70
-- row: b, 70, 70
-- end-expected
SELECT g, sum(v), nth_value(sum(v), 2)
OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM wpl_t GROUP BY g ORDER BY g;

-- stmt 48: several windows in one grouped query, and one inside a larger expression
-- begin-expected
-- columns: g | sum | rank | row_number | sum
-- row: a, 50, 1, 2, 120
-- row: b, 70, 2, 1, 120
-- end-expected
SELECT g, sum(v), rank() OVER (ORDER BY sum(v)), row_number() OVER (ORDER BY sum(v) DESC),
sum(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | ?column?
-- row: a, 51
-- row: b, 72
-- end-expected
SELECT g, sum(v) + rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | case
-- row: a, low
-- row: b, high
-- end-expected
SELECT g, CASE WHEN rank() OVER (ORDER BY sum(v)) = 1 THEN 'low' ELSE 'high' END
FROM wpl_t GROUP BY g ORDER BY g;

-- stmt 49: a named window carries the aggregate exactly as an inline one does
-- begin-expected
-- columns: g | sum
-- row: a, 50
-- row: b, 120
-- end-expected
SELECT g, sum(sum(v)) OVER w FROM wpl_t GROUP BY g WINDOW w AS (ORDER BY sum(v)) ORDER BY g;

-- stmt 50: FILTER on a window aggregate reads the grouped row too
-- begin-expected
-- columns: g | sum
-- row: a, 50
-- row: b, 50
-- end-expected
SELECT g, sum(sum(v)) FILTER (WHERE g = 'a') OVER () FROM wpl_t GROUP BY g ORDER BY g;

-- stmt 51: an ordered-set aggregate under a window is a value like any other
-- begin-expected
-- columns: g | p | rank
-- row: a, 20, 1
-- row: b, 35, 2
-- end-expected
SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) p,
rank() OVER (ORDER BY percentile_cont(0.5) WITHIN GROUP (ORDER BY v))
FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | sum
-- row: a, 50
-- row: b, 50
-- end-expected
SELECT g, sum(percentile_disc(0.5) WITHIN GROUP (ORDER BY v)) OVER ()
FROM wpl_t GROUP BY g ORDER BY g;

-- ============================================================================
-- 11. the clauses around a window over grouped rows
-- ============================================================================

-- stmt 52: the window runs on the groups HAVING kept
-- begin-expected
-- columns: g | sum | sum
-- row: b, 70, 70
-- end-expected
SELECT g, sum(v), sum(sum(v)) OVER () FROM wpl_t GROUP BY g HAVING sum(v) > 60 ORDER BY g;

-- begin-expected
-- columns: g | sum
-- row: b, 70
-- row: a, 50
-- end-expected
SELECT g, sum(v) FROM wpl_t GROUP BY g HAVING sum(v) > 0
ORDER BY rank() OVER (ORDER BY sum(v) DESC);

-- stmt 53: with ROLLUP, GROUPING SETS and CUBE the window runs over every row they spell
-- begin-expected
-- columns: g | sum | rank
-- row: a, 50, 1
-- row: b, 70, 2
-- row: NULL, 120, 3
-- end-expected
SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY ROLLUP(g) ORDER BY 1,2,3;

-- begin-expected
-- columns: g | sum | sum
-- row: a, 50, 240
-- row: b, 70, 240
-- row: NULL, 120, 240
-- end-expected
SELECT g, sum(v), sum(sum(v)) OVER () FROM wpl_t GROUP BY GROUPING SETS ((g),())
ORDER BY 1,2,3;

-- begin-expected
-- columns: g | i | sum | rank
-- row: a, 1, 10, 1
-- row: a, 2, 20, 3
-- row: a, 3, 20, 3
-- row: a, NULL, 50, 11
-- row: b, 4, 30, 7
-- row: b, 5, 40, 9
-- row: b, NULL, 70, 12
-- row: NULL, 1, 10, 1
-- row: NULL, 2, 20, 3
-- row: NULL, 3, 20, 3
-- row: NULL, 4, 30, 7
-- row: NULL, 5, 40, 9
-- row: NULL, NULL, 120, 13
-- end-expected
SELECT g, i, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY CUBE(g,i)
ORDER BY 1,2,3,4;

-- begin-expected
-- columns: g | grouping | rank
-- row: a, 0, 1
-- row: b, 0, 2
-- row: NULL, 1, 3
-- end-expected
SELECT g, grouping(g), rank() OVER (ORDER BY grouping(g), sum(v)) FROM wpl_t
GROUP BY ROLLUP(g) ORDER BY 1,2,3;

-- stmt 54: DISTINCT, ORDER BY and LIMIT all read the window already computed
-- begin-expected
-- columns: sum
-- row: 120
-- end-expected
SELECT DISTINCT sum(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY 1;

-- begin-expected
-- columns: sum
-- row: 50
-- row: 70
-- end-expected
SELECT DISTINCT sum(sum(v)) OVER (PARTITION BY g) FROM wpl_t GROUP BY g ORDER BY 1;

-- begin-expected
-- columns: g | sum | rank
-- row: b, 70, 2
-- row: a, 50, 1
-- end-expected
SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY 3 DESC, 1;

-- begin-expected
-- columns: g | sum | rank
-- row: b, 70, 2
-- end-expected
SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g LIMIT 1 OFFSET 1;

-- ============================================================================
-- 12. what a window over a grouped result still may not be
-- ============================================================================

-- stmt 55: a nested aggregate with no OVER has no second pass to fold it, and is refused
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT sum(sum(v)) FROM wpl_t GROUP BY g;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT g, max(sum(v)) FROM wpl_t GROUP BY g;

-- refused while the statement is read, so an empty result refuses it too
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT sum(sum(v)) FROM wpl_t WHERE 1 = 0 GROUP BY g;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT g FROM wpl_t GROUP BY g HAVING sum(sum(v)) > 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT g FROM wpl_t GROUP BY g ORDER BY sum(sum(v));

-- an ordered-set aggregate reads its WITHIN GROUP ORDER BY per input row, like any other
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sum(v)) FROM wpl_t GROUP BY g;

-- stmt 56: a window lifts exactly one level, so two aggregates under it are still nested
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT sum(sum(sum(v))) OVER () FROM wpl_t GROUP BY g;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT rank() OVER (ORDER BY sum(sum(v))) FROM wpl_t GROUP BY g;

-- stmt 57: a window specification is judged against the grouping like anything else
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT g, rank() OVER (ORDER BY v) FROM wpl_t GROUP BY g;

-- an aggregate under the window groups the query, so every other target must be grouped
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT i, rank() OVER (ORDER BY sum(v)) FROM wpl_t;

-- a WINDOW clause entry is judged even when nothing names it
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT 1 FROM wpl_t GROUP BY g WINDOW w AS (ORDER BY v);

-- stmt 58: and the placements already refused are refused here too
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in HAVING
-- end-expected-error
SELECT g, sum(v) FROM wpl_t GROUP BY g HAVING rank() OVER (ORDER BY sum(v)) = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT i FROM wpl_t WHERE rank() OVER (ORDER BY sum(v)) = 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in FILTER
-- end-expected-error
SELECT g, sum(sum(v)) FILTER (WHERE sum(v) > 60) OVER () FROM wpl_t GROUP BY g;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot contain window function calls
-- end-expected-error
SELECT sum(sum(v) OVER ()) FROM wpl_t GROUP BY g;

-- stmt 59: the ordinary grouped shapes are unchanged
-- begin-expected
-- columns: g | sum
-- row: a, 50
-- row: b, 70
-- end-expected
SELECT g, sum(v) FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: g | sum
-- row: b, 70
-- row: a, 50
-- end-expected
SELECT g, sum(v) FROM wpl_t GROUP BY g ORDER BY sum(v) DESC;

-- begin-expected
-- columns: g | count
-- row: a, 2
-- row: b, 2
-- end-expected
SELECT g, count(*) OVER () FROM wpl_t GROUP BY g ORDER BY g;

-- begin-expected
-- columns: count | rank
-- row: 0, 1
-- end-expected
SELECT count(*), rank() OVER (ORDER BY count(*)) FROM wpl_t WHERE 1 = 0;

-- cleanup
DROP TABLE wpl_t;
