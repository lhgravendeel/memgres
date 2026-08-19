-- ============================================================================
-- A window function's offsets are read where they are written
-- ============================================================
--
-- lag, lead, nth_value and ntile each take a count, and every frame bound takes a
-- size. Two things decide what those are worth: the type the argument was written
-- with, which says whether the call exists at all, and the row it is read on, which
-- says what it is worth there. An offset that names a column steps a different
-- distance on every row; one written as a bigint is not an integer and names no
-- function; one written as a quoted literal is not yet anything and becomes what the
-- parameter asks for.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS win_off;
CREATE TABLE win_off (id int, v int, o int, s smallint, d date, n numeric);
INSERT INTO win_off VALUES (1,10,1,1,'2020-01-01',1.5),(2,20,2,2,'2020-01-02',2.5),(3,30,0,1,'2020-01-04',3.5),(4,40,1,1,'2020-01-08',4.5);

-- ============================================================================
-- an offset is read on the row it produces a value for
-- ============================================================================

-- lag(v, o) steps o rows back from each row, and o differs on each of them.
-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | 30
-- row: 4 | 30
-- end-expected
SELECT id, lag(v, o) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | lead
-- row: 1 | 20
-- row: 2 | 40
-- row: 3 | 30
-- row: 4 | NULL
-- end-expected
SELECT id, lead(v, o) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- An offset of zero is the row itself.
-- begin-expected
-- columns: id | lag
-- row: 1 | 10
-- row: 2 | 10
-- row: 3 | 40
-- row: 4 | 40
-- end-expected
SELECT id, lag(v, o - 1) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- The default is read on the same row, so it may name a column too.
-- begin-expected
-- columns: id | lag
-- row: 1 | -1
-- row: 2 | -1
-- row: 3 | 30
-- row: 4 | 30
-- end-expected
SELECT id, lag(v, o, -1) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | lag
-- row: 1 | 10
-- row: 2 | 10
-- row: 3 | 20
-- row: 4 | 30
-- end-expected
SELECT id, lag(v, 1, v) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- No offset at all is one row back.
-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | 10
-- row: 3 | 20
-- row: 4 | 30
-- end-expected
SELECT id, lag(v) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- ============================================================================
-- a null offset produces null, and the default does not answer for it
-- ============================================================================

-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | NULL
-- row: 4 | NULL
-- end-expected
SELECT id, lag(v, NULL) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | NULL
-- row: 4 | NULL
-- end-expected
SELECT id, lag(v, NULL, -1) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | lead
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | NULL
-- row: 4 | NULL
-- end-expected
SELECT id, lead(v, NULL) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- ============================================================================
-- the type an offset was written with says whether the function exists
-- ============================================================================

-- smallint widens to integer, so the call is the integer one.
-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | 20
-- row: 4 | 30
-- end-expected
SELECT id, lag(v, s) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | ntile
-- row: 1 | 1
-- row: 2 | 1
-- row: 3 | 1
-- row: 4 | 1
-- end-expected
SELECT id, ntile(s) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- bigint does not narrow to integer, and there is no such function.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, lag(v, v::bigint) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, lag(v, 2::bigint) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, ntile(v::bigint) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, nth_value(v, 2::bigint) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- A whole number is written as the narrowest type that holds it, so one past the
-- integer range is a bigint and names no function either.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, lag(v, 2147483648) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, ntile(2147483648) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, nth_value(v, 3000000000) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- numeric does not narrow either, whether or not it has a fraction.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, lag(v, 1.0) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, lag(v, 1.5) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, ntile(2.5) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id, nth_value(v, 2.5) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- Written as a cast to integer it is an integer, whatever it was before.
-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | 10
-- row: 4 | 20
-- end-expected
SELECT id, lag(v, 2.0::int) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- ============================================================================
-- a quoted literal is not yet anything, and becomes what the parameter asks for
-- ============================================================================

-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | 10
-- row: 3 | 20
-- row: 4 | 30
-- end-expected
SELECT id, lag(v, '1') OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | ntile
-- row: 1 | 1
-- row: 2 | 1
-- row: 3 | 2
-- row: 4 | 2
-- end-expected
SELECT id, ntile('2') OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | nth_value
-- row: 1 | 20
-- row: 2 | 20
-- row: 3 | 20
-- row: 4 | 20
-- end-expected
SELECT id, nth_value(v, '2') OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off ORDER BY id;

-- And says so when it does not read as one.
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT id, ntile('x') OVER (ORDER BY id) FROM win_off ORDER BY id;

-- ============================================================================
-- ntile counts buckets once for the whole partition
-- ============================================================================

-- begin-expected
-- columns: id | ntile
-- row: 1 | 1
-- row: 2 | 1
-- row: 3 | 1
-- row: 4 | 1
-- end-expected
SELECT id, ntile(o) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | ntile
-- row: 1 | 1
-- row: 2 | 1
-- row: 3 | 2
-- row: 4 | 2
-- end-expected
SELECT id, ntile(2) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22014
-- end-expected-error
SELECT id, ntile(0) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | ntile
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | NULL
-- row: 4 | NULL
-- end-expected
SELECT id, ntile(NULL) OVER (ORDER BY id) FROM win_off ORDER BY id;

-- ============================================================================
-- nth_value reads its position on each row
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22016
-- end-expected-error
SELECT id, nth_value(v, o) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22016
-- end-expected-error
SELECT id, nth_value(v, 0) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | nth_value
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | NULL
-- row: 4 | NULL
-- end-expected
SELECT id, nth_value(v, NULL) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off ORDER BY id;

-- ============================================================================
-- a frame size is counted in the type the frame counts in
-- ============================================================================

-- ROWS and GROUPS count in bigints, so a quoted size is read as one -- including
-- its sign, which decided nothing while it stayed text.
-- begin-expected
-- columns: id | count
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 2
-- row: 4 | 2
-- end-expected
SELECT id, count(*) OVER (ORDER BY id ROWS '1' PRECEDING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS '-1' PRECEDING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS -1 PRECEDING) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | count
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 2
-- row: 4 | 2
-- end-expected
SELECT id, count(*) OVER (ORDER BY id GROUPS '1' PRECEDING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id GROUPS '-2' PRECEDING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id GROUPS -1 PRECEDING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND '-1' FOLLOWING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS 'x' PRECEDING) FROM win_off ORDER BY id;

-- A size past the bigint range is no bigint, rather than a small one wrapped round.
-- begin-expected
-- columns: id | count
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 3
-- row: 4 | 4
-- end-expected
SELECT id, count(*) OVER (ORDER BY id ROWS 2000000000000 PRECEDING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS 9223372036854775808 PRECEDING) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS 99999999999999999999 PRECEDING) FROM win_off ORDER BY id;

-- A null size covers no rows beyond the current one.
-- begin-expected-error
-- sqlstate: 22004
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS NULL PRECEDING) FROM win_off ORDER BY id;

-- A frame size is one answer for the whole window, so it may not name a column.
-- begin-expected-error
-- sqlstate: 42P10
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS o PRECEDING) FROM win_off ORDER BY id;

-- ============================================================================
-- a RANGE size is read in whatever the ordering column is measured in
-- ============================================================================

-- begin-expected
-- columns: id | count
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 1
-- row: 4 | 1
-- end-expected
SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '-1 day' PRECEDING AND CURRENT ROW) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | count
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 2
-- row: 4 | 2
-- end-expected
SELECT id, count(*) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | count
-- row: 1 | 1
-- row: 2 | 1
-- row: 3 | 1
-- row: 4 | 1
-- end-expected
SELECT id, count(*) OVER (ORDER BY v RANGE BETWEEN '5' PRECEDING AND CURRENT ROW) FROM win_off ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY v RANGE BETWEEN '-5' PRECEDING AND CURRENT ROW) FROM win_off ORDER BY id;

-- ============================================================================
-- what already worked still does
-- ============================================================================

-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | 10
-- row: 4 | 20
-- end-expected
SELECT id, lag(v, 2) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | lead
-- row: 1 | 30
-- row: 2 | 40
-- row: 3 | 0
-- row: 4 | 0
-- end-expected
SELECT id, lead(v, 2, 0) OVER (ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | first_value
-- row: 1 | 10
-- row: 2 | 10
-- row: 3 | 20
-- row: 4 | 30
-- end-expected
SELECT id, first_value(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | last_value
-- row: 1 | 20
-- row: 2 | 30
-- row: 3 | 40
-- row: 4 | 40
-- end-expected
SELECT id, last_value(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | nth_value
-- row: 1 | 20
-- row: 2 | 20
-- row: 3 | 20
-- row: 4 | 20
-- end-expected
SELECT id, nth_value(v, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | count
-- row: 1 | 2
-- row: 2 | 3
-- row: 3 | 3
-- row: 4 | 2
-- end-expected
SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | sum
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 40
-- row: 4 | 80
-- end-expected
SELECT id, sum(v) OVER (PARTITION BY s ORDER BY id) FROM win_off ORDER BY id;
-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | 10
-- row: 4 | 30
-- end-expected
SELECT id, lag(v) OVER (PARTITION BY s ORDER BY id) FROM win_off ORDER BY id;

-- teardown
DROP TABLE IF EXISTS win_off;
