-- Window specification validation, and the evaluation cases around it:
-- 1. named windows -- undefined name, duplicate definition, illegal copies
-- 2. frame clauses -- RANGE/GROUPS offsets vs ORDER BY shape, impossible bounds, bad offsets
-- 3. placement -- window functions in GROUP BY, nested calls, FILTER on non-aggregates
-- 4. lag/lead with a negative, zero, NULL or out-of-range offset
-- 5. the frames themselves (ROWS/RANGE/GROUPS, all four EXCLUDE variants), which must not change

-- setup
CREATE TABLE wfv_t (i int, g text, v int);
INSERT INTO wfv_t VALUES (1,'a',10),(2,'a',20),(3,'a',20),(4,'b',30),(5,'b',40);
CREATE TABLE wfv_n (i int, v int);
INSERT INTO wfv_n VALUES (1,10),(2,NULL),(3,30),(4,NULL),(5,50);

-- ============================================================================
-- 1. Named windows
-- ============================================================================

-- stmt 1: OVER an undefined window name
-- begin-expected-error
-- sqlstate: 42704
-- message-like: window "nosuchwindow" does not exist
-- end-expected-error
SELECT sum(v) OVER nosuchwindow FROM wfv_t;

-- stmt 2: OVER (undefined ...) is the same rejection
-- begin-expected-error
-- sqlstate: 42704
-- message-like: window "nosuch" does not exist
-- end-expected-error
SELECT sum(v) OVER (nosuch ORDER BY i) FROM wfv_t;

-- stmt 3: a window name referenced with no WINDOW clause at all
-- begin-expected-error
-- sqlstate: 42704
-- message-like: window "w" does not exist
-- end-expected-error
SELECT sum(v) OVER w FROM wfv_t;

-- stmt 4: a WINDOW entry may not name a window that does not exist either
-- begin-expected-error
-- sqlstate: 42704
-- message-like: window "nosuch" does not exist
-- end-expected-error
SELECT 1 FROM wfv_t WINDOW w AS (nosuch);

-- stmt 5: the same window name defined twice
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window "w" is already defined
-- end-expected-error
SELECT sum(v) OVER w FROM wfv_t WINDOW w AS (), w AS ();

-- stmt 6: duplicate names are rejected whatever the two definitions say
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window "w" is already defined
-- end-expected-error
SELECT sum(v) OVER w FROM wfv_t WINDOW w AS (PARTITION BY g), w AS (ORDER BY i);

-- stmt 7: a copy may not restate PARTITION BY
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: cannot override PARTITION BY clause of window "w"
-- end-expected-error
SELECT sum(v) OVER (w PARTITION BY g) FROM wfv_t WINDOW w AS (PARTITION BY g);

-- stmt 8: not even when the named window has no PARTITION BY of its own
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: cannot override PARTITION BY clause of window "w"
-- end-expected-error
SELECT sum(v) OVER (w PARTITION BY i) FROM wfv_t WINDOW w AS (ORDER BY i);

-- stmt 9: a copy may not restate an ORDER BY the original already fixed
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: cannot override ORDER BY clause of window "w"
-- end-expected-error
SELECT sum(v) OVER (w ORDER BY i) FROM wfv_t WINDOW w AS (ORDER BY v);

-- stmt 10: a window carrying a frame cannot be copied at all
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: cannot copy window "w" because it has a frame clause
-- end-expected-error
SELECT sum(v) OVER (w) FROM wfv_t WINDOW w AS (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

-- stmt 11: the same rule applies to WINDOW w2 AS (w)
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: cannot copy window "w" because it has a frame clause
-- end-expected-error
SELECT sum(v) OVER w2 FROM wfv_t
  WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), w2 AS (w);

-- stmt 12: and to overriding through WINDOW w2 AS (w ...)
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: cannot override ORDER BY clause of window "w"
-- end-expected-error
SELECT sum(v) OVER w2 FROM wfv_t WINDOW w AS (ORDER BY i), w2 AS (w ORDER BY v);

-- stmt 13: a bare OVER w uses the named window frame and all
-- begin-expected
-- columns: s
-- row: 10
-- row: 30
-- row: 40
-- row: 50
-- row: 70
-- end-expected
SELECT sum(v) OVER w AS s FROM wfv_t
  WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY i;

-- stmt 14: adding an ORDER BY to a window that has none is allowed
-- begin-expected
-- columns: i | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 30
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (w ORDER BY i) AS s FROM wfv_t WINDOW w AS (PARTITION BY g) ORDER BY i;

-- stmt 15: adding a frame to a window that has none is allowed
-- begin-expected
-- columns: i | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 40
-- row: 4, 30
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (w ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s
FROM wfv_t WINDOW w AS (PARTITION BY g ORDER BY i) ORDER BY i;

-- stmt 16: WINDOW w2 AS (w ORDER BY ...) inherits the partitioning
-- begin-expected
-- columns: i | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 30
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER w2 AS s FROM wfv_t
  WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY i) ORDER BY i;

-- stmt 17: window names are case-insensitive, and an unused entry is harmless
-- begin-expected
-- columns: i | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 80
-- row: 5, 120
-- end-expected
SELECT i, sum(v) OVER W AS s FROM wfv_t WINDOW w AS (ORDER BY i), x AS () ORDER BY i;

-- ============================================================================
-- 2. Frame clauses
-- ============================================================================

-- stmt 18: a RANGE offset needs exactly one ORDER BY column, not two
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column
-- end-expected-error
SELECT sum(v) OVER (ORDER BY g, v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 19: nor none at all
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column
-- end-expected-error
SELECT sum(v) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 20: the rule follows the named window the frame is attached to
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column
-- end-expected-error
SELECT sum(v) OVER (w RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t
  WINDOW w AS (PARTITION BY g);

-- stmt 21: GROUPS counts peer groups, so it needs an ORDER BY
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: GROUPS mode requires an ORDER BY clause
-- end-expected-error
SELECT sum(v) OVER (GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 22: a start after the end -- current row cannot have preceding rows
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: frame starting from current row cannot have preceding rows
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM wfv_t;

-- stmt 23: a start after the end -- following row cannot have preceding rows
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: frame starting from following row cannot have preceding rows
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM wfv_t;

-- stmt 24: the frame cannot start at the end of the partition
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: frame start cannot be UNBOUNDED FOLLOWING
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM wfv_t;

-- stmt 25: nor end at its beginning
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: frame end cannot be UNBOUNDED PRECEDING
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM wfv_t;

-- stmt 26: the shape is checked inside a WINDOW clause too
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: frame starting from current row cannot have preceding rows
-- end-expected-error
SELECT 1 FROM wfv_t WINDOW w AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 PRECEDING);

-- stmt 27: a NULL start offset
-- begin-expected-error
-- sqlstate: 22004
-- message-like: frame starting offset must not be null
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 28: a NULL end offset
-- begin-expected-error
-- sqlstate: 22004
-- message-like: frame ending offset must not be null
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND NULL FOLLOWING) FROM wfv_t;

-- stmt 29: NULL is rejected in GROUPS mode as well
-- begin-expected-error
-- sqlstate: 22004
-- message-like: frame starting offset must not be null
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 30: and in RANGE mode
-- begin-expected-error
-- sqlstate: 22004
-- message-like: frame starting offset must not be null
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 31: a negative ROWS offset is a size error, not a syntax error
-- begin-expected-error
-- sqlstate: 22013
-- message-like: frame starting offset must not be negative
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 32: the end offset is checked the same way
-- begin-expected-error
-- sqlstate: 22013
-- message-like: frame ending offset must not be negative
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND -1 FOLLOWING) FROM wfv_t;

-- stmt 33: GROUPS offsets are sizes too
-- begin-expected-error
-- sqlstate: 22013
-- message-like: frame starting offset must not be negative
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 34: RANGE reports a negative size in its own words
-- begin-expected-error
-- sqlstate: 22013
-- message-like: invalid preceding or following size in window function
-- end-expected-error
SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 35: a RANGE offset needs a sort column it can be added to
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: RANGE with offset PRECEDING/FOLLOWING is not supported for column type text
-- end-expected-error
SELECT sum(v) OVER (ORDER BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 36: and an offset of the sort column's own type
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: is not supported for column type integer and offset type numeric
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM wfv_t;

-- stmt 37: cast the sort column and the same offset is fine
-- begin-expected
-- columns: s
-- row: 10
-- row: 30
-- row: 40
-- row: 40
-- row: 40
-- end-expected
SELECT sum(v) OVER (ORDER BY v::numeric RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) AS s
FROM wfv_t ORDER BY 1;

-- stmt 38: a ROWS offset is a bigint, so a fractional one rounds
-- begin-expected
-- columns: i | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 70
-- row: 5, 90
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1.5 PRECEDING AND CURRENT ROW) AS s
FROM wfv_t ORDER BY i;

-- stmt 39: an explicitly typed offset is accepted
-- begin-expected
-- columns: i | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 50
-- row: 4, 70
-- row: 5, 90
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 2::bigint PRECEDING AND CURRENT ROW) AS s
FROM wfv_t ORDER BY i;

-- ============================================================================
-- 3. Placement
-- ============================================================================

-- stmt 40: a window function cannot be grouped by
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in GROUP BY
-- end-expected-error
SELECT g FROM wfv_t GROUP BY rank() OVER (ORDER BY v);

-- stmt 41: window function calls cannot be nested
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window function calls cannot be nested
-- end-expected-error
SELECT sum(rank() OVER (ORDER BY v)) OVER () FROM wfv_t;

-- stmt 42: nor appear in another window's ORDER BY
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT rank() OVER (ORDER BY row_number() OVER ()) FROM wfv_t;

-- stmt 43: nor in another window's PARTITION BY
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT rank() OVER (PARTITION BY rank() OVER ()) FROM wfv_t;

-- stmt 44: FILTER is not implemented for a non-aggregate window function
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FILTER is not implemented for non-aggregate window functions
-- end-expected-error
SELECT rank() FILTER (WHERE v > 15) OVER (ORDER BY v) FROM wfv_t;

-- stmt 45: including nth_value
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FILTER is not implemented for non-aggregate window functions
-- end-expected-error
SELECT nth_value(v, 2) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wfv_t;

-- stmt 46: and when the window comes from a WINDOW clause
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FILTER is not implemented for non-aggregate window functions
-- end-expected-error
SELECT rank() FILTER (WHERE v > 15) OVER w FROM wfv_t WINDOW w AS (ORDER BY v);

-- stmt 47: FILTER on a window aggregate still works
-- begin-expected
-- columns: i | s
-- row: 1, NULL
-- row: 2, 20
-- row: 3, 40
-- row: 4, 70
-- row: 5, 110
-- end-expected
SELECT i, sum(v) FILTER (WHERE v > 15) OVER (ORDER BY i) AS s FROM wfv_t ORDER BY i;

-- stmt 48: a window function in WHERE stays rejected
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT i FROM wfv_t WHERE rank() OVER (ORDER BY v) = 1;

-- stmt 49: and one in HAVING
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in HAVING
-- end-expected-error
SELECT g FROM wfv_t GROUP BY g HAVING rank() OVER (ORDER BY g) = 1;

-- stmt 50: a window function in ORDER BY is fine
-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
SELECT i FROM wfv_t ORDER BY rank() OVER (ORDER BY v), i;

-- ============================================================================
-- 4. lag / lead offsets
-- ============================================================================

-- stmt 51: a negative lag looks forward
-- begin-expected
-- columns: i | x
-- row: 1, 20
-- row: 2, 20
-- row: 3, 30
-- row: 4, 40
-- row: 5, NULL
-- end-expected
SELECT i, lag(v, -1) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 52: past the end of the partition it is just NULL
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, lag(v, -10) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 53: the default argument applies to a negative offset too
-- begin-expected
-- columns: i | x
-- row: 1, 20
-- row: 2, 20
-- row: 3, 30
-- row: 4, 40
-- row: 5, 0
-- end-expected
SELECT i, lag(v, -1, 0) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 54: a negative lead looks backward
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, 10
-- row: 3, 20
-- row: 4, 20
-- row: 5, 30
-- end-expected
SELECT i, lead(v, -1) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 55: with a default
-- begin-expected
-- columns: i | x
-- row: 1, 0
-- row: 2, 10
-- row: 3, 20
-- row: 4, 20
-- row: 5, 30
-- end-expected
SELECT i, lead(v, -1, 0) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 56: a negative offset stays inside its own partition
-- begin-expected
-- columns: i | x
-- row: 1, 20
-- row: 2, 20
-- row: 3, NULL
-- row: 4, 40
-- row: 5, NULL
-- end-expected
SELECT i, lag(v, -1) OVER (PARTITION BY g ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 57: an offset at the top of integer does not wrap around
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, lead(v, 2147483647) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 58: nor at the bottom
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, lag(v, -2147483648) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 59: a zero offset is the current row
-- begin-expected
-- columns: i | x
-- row: 1, 10
-- row: 2, 20
-- row: 3, 20
-- row: 4, 30
-- row: 5, 40
-- end-expected
SELECT i, lag(v, 0) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 60: a NULL offset yields NULL, ignoring the default
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, lag(v, NULL) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 61: the same for lead
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, lead(v, NULL) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 62: the offset is an integer, so a fractional one matches no function
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lag(integer, numeric) does not exist
-- end-expected-error
SELECT lag(v, 1.5) OVER (ORDER BY i) FROM wfv_t;

-- stmt 63: positive offsets and defaults are unaffected
-- begin-expected
-- columns: i | x
-- row: 1, -1
-- row: 2, -1
-- row: 3, 10
-- row: 4, 20
-- row: 5, 20
-- end-expected
SELECT i, lag(v, 2, -1) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 64: lag over NULL data keeps the NULLs
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, 10
-- row: 3, NULL
-- row: 4, 30
-- row: 5, NULL
-- end-expected
SELECT i, lag(v) OVER (ORDER BY i) AS x FROM wfv_n ORDER BY i;

-- ============================================================================
-- 5. nth_value and ntile
-- ============================================================================

-- stmt 65: nth_value with a NULL position yields NULL
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, nth_value(v, NULL) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 66: a fractional position matches no function
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nth_value(integer, numeric) does not exist
-- end-expected-error
SELECT nth_value(v, 2.5) OVER (ORDER BY i) FROM wfv_t;

-- stmt 67: zero is still rejected
-- begin-expected-error
-- sqlstate: 22016
-- message-like: argument of nth_value must be greater than zero
-- end-expected-error
SELECT nth_value(v, 0) OVER (ORDER BY i) FROM wfv_t;

-- stmt 68: a position past the frame is NULL
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, nth_value(v, 10) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS x
FROM wfv_t ORDER BY i;

-- stmt 69: and a position inside it is the value
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, 20
-- row: 3, 20
-- row: 4, 20
-- row: 5, 20
-- end-expected
SELECT i, nth_value(v, 2) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 70: ntile has a SQLSTATE of its own for a non-positive bucket count
-- begin-expected-error
-- sqlstate: 22014
-- message-like: argument of ntile must be greater than zero
-- end-expected-error
SELECT ntile(0) OVER (ORDER BY i) FROM wfv_t;

-- stmt 71: a NULL bucket count yields NULL rather than an error
-- begin-expected
-- columns: i | x
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT i, ntile(NULL) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- stmt 72: a real bucket count is unaffected
-- begin-expected
-- columns: i | x
-- row: 1, 1
-- row: 2, 1
-- row: 3, 1
-- row: 4, 2
-- row: 5, 2
-- end-expected
SELECT i, ntile(2) OVER (ORDER BY i) AS x FROM wfv_t ORDER BY i;

-- ============================================================================
-- 6. The frames themselves, which must keep working
-- ============================================================================

-- stmt 73: ROWS frame
-- begin-expected
-- columns: i | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 40
-- row: 4, 50
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS 1 PRECEDING) AS s FROM wfv_t ORDER BY i;

-- stmt 74: a ROWS frame entirely in the past, empty at the first row
-- begin-expected
-- columns: i | s
-- row: 1, NULL
-- row: 2, 10
-- row: 3, 30
-- row: 4, 40
-- row: 5, 50
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS s
FROM wfv_t ORDER BY i;

-- stmt 75: a ROWS frame entirely in the future, empty at the last row
-- begin-expected
-- columns: i | s
-- row: 1, 40
-- row: 2, 50
-- row: 3, 70
-- row: 4, 40
-- row: 5, NULL
-- end-expected
SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS s
FROM wfv_t ORDER BY i;

-- stmt 76: count over an empty frame is 0, not NULL
-- begin-expected
-- columns: i | c
-- row: 1, 2
-- row: 2, 2
-- row: 3, 1
-- row: 4, 0
-- row: 5, 0
-- end-expected
SELECT i, count(*) OVER (ORDER BY i ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS c
FROM wfv_t ORDER BY i;

-- stmt 77: RANGE frame with an offset
-- begin-expected
-- columns: i | s
-- row: 1, 50
-- row: 2, 80
-- row: 3, 80
-- row: 4, 110
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) AS s
FROM wfv_t ORDER BY i;

-- stmt 78: GROUPS frame with an offset
-- begin-expected
-- columns: i | s
-- row: 1, 50
-- row: 2, 80
-- row: 3, 80
-- row: 4, 110
-- row: 5, 70
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
FROM wfv_t ORDER BY i;

-- stmt 79: EXCLUDE CURRENT ROW
-- begin-expected
-- columns: i | s
-- row: 1, 110
-- row: 2, 100
-- row: 3, 100
-- row: 4, 90
-- row: 5, 80
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS s
FROM wfv_t ORDER BY i;

-- stmt 80: EXCLUDE GROUP
-- begin-expected
-- columns: i | s
-- row: 1, 110
-- row: 2, 80
-- row: 3, 80
-- row: 4, 90
-- row: 5, 80
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS s
FROM wfv_t ORDER BY i;

-- stmt 81: EXCLUDE TIES
-- begin-expected
-- columns: i | s
-- row: 1, 120
-- row: 2, 100
-- row: 3, 100
-- row: 4, 120
-- row: 5, 120
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS s
FROM wfv_t ORDER BY i;

-- stmt 82: EXCLUDE NO OTHERS
-- begin-expected
-- columns: i | s
-- row: 1, 120
-- row: 2, 120
-- row: 3, 120
-- row: 4, 120
-- row: 5, 120
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) AS s
FROM wfv_t ORDER BY i;

-- stmt 83: EXCLUDE GROUP under a GROUPS frame
-- begin-expected
-- columns: i | s
-- row: 1, 110
-- row: 2, 80
-- row: 3, 80
-- row: 4, 90
-- row: 5, 80
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS s
FROM wfv_t ORDER BY i;

-- stmt 84: EXCLUDE TIES under a RANGE frame
-- begin-expected
-- columns: i | s
-- row: 1, 120
-- row: 2, 100
-- row: 3, 100
-- row: 4, 120
-- row: 5, 120
-- end-expected
SELECT i, sum(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS s
FROM wfv_t ORDER BY i;

-- stmt 85: the ranking functions over a shared named window
-- begin-expected
-- columns: i | r | dr
-- row: 1, 1, 1
-- row: 2, 2, 2
-- row: 3, 2, 2
-- row: 4, 4, 3
-- row: 5, 5, 4
-- end-expected
SELECT i, rank() OVER w AS r, dense_rank() OVER w AS dr
FROM wfv_t WINDOW w AS (ORDER BY v) ORDER BY i;

-- stmt 86: first_value and last_value over a sliding frame
-- begin-expected
-- columns: i | f | l
-- row: 1, 10, 20
-- row: 2, 10, 20
-- row: 3, 20, 30
-- row: 4, 20, 40
-- row: 5, 30, 40
-- end-expected
SELECT i,
       first_value(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS f,
       last_value(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS l
FROM wfv_t ORDER BY i;

-- cleanup
DROP TABLE wfv_t;
DROP TABLE wfv_n;
