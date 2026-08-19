-- ============================================================================
-- A row count is read before it is used
--
-- The count a limit was written with settles how many rows come back before any
-- of them is looked at, and WITH TIES then keeps whatever is tied with the last
-- of those -- so where the count is none there is no last row, and nothing to be
-- tied with it. memgres read that row anyway, off the front of the list, and the
-- client saw an internal fault. A window offset is a count in the same way: it
-- is a value of the type the parameter declares, read on the row the answer is
-- being produced for, so it may be a column of that row and may not be a number
-- too large to be one. Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS zz_lt CASCADE;
CREATE TABLE zz_lt (id int, v int);
INSERT INTO zz_lt VALUES (1, 10), (2, 20), (3, 20), (4, 30);

-- ============================================================================
-- A limit of none keeps none, and leaves nothing for the rest to be tied with
-- ============================================================================

-- begin-expected
-- columns: id
-- end-expected
SELECT id FROM zz_lt ORDER BY id FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: id | count
-- end-expected
SELECT id, count(*) FROM zz_lt GROUP BY id ORDER BY id FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: a
-- end-expected
SELECT 1 AS a ORDER BY a FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: id | r
-- end-expected
SELECT id, row_number() OVER (ORDER BY id) AS r FROM zz_lt ORDER BY id FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: s
-- end-expected
SELECT generate_series(1, id) AS s FROM zz_lt ORDER BY s FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: id
-- end-expected
SELECT id FROM zz_lt ORDER BY id OFFSET 1 FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: id
-- end-expected
SELECT id FROM zz_lt UNION SELECT id FROM zz_lt ORDER BY id FETCH FIRST 0 ROWS WITH TIES;

-- The same count without the ties, which is the answer WITH TIES has to agree with.
-- begin-expected
-- columns: id
-- end-expected
SELECT id FROM zz_lt ORDER BY id LIMIT 0;
-- begin-expected
-- columns: v
-- end-expected
SELECT v FROM zz_lt ORDER BY v FETCH FIRST 0 ROWS ONLY;

-- ============================================================================
-- A limit of some keeps every row tied with the last of them
-- ============================================================================

-- begin-expected
-- columns: v
-- row: 10
-- end-expected
SELECT v FROM zz_lt ORDER BY v FETCH FIRST 1 ROWS WITH TIES;
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- row: 20
-- end-expected
SELECT v FROM zz_lt ORDER BY v FETCH FIRST 2 ROWS WITH TIES;
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- row: 20
-- end-expected
SELECT v FROM zz_lt ORDER BY v FETCH FIRST 3 ROWS WITH TIES;
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- row: 20
-- row: 30
-- end-expected
SELECT v FROM zz_lt ORDER BY v FETCH FIRST 10 ROWS WITH TIES;
-- begin-expected
-- columns: v
-- row: 30
-- end-expected
SELECT v FROM zz_lt ORDER BY v DESC FETCH FIRST 1 ROWS WITH TIES;
-- begin-expected
-- columns: v
-- row: 20
-- row: 20
-- end-expected
SELECT v FROM zz_lt ORDER BY v OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;
-- begin-expected
-- columns: v
-- end-expected
SELECT v FROM zz_lt ORDER BY v OFFSET 10 FETCH FIRST 1 ROWS WITH TIES;

-- A tie is a tie under the sort that was written, not under the whole row.
-- begin-expected
-- columns: id | v
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 20
-- end-expected
SELECT id, v FROM zz_lt ORDER BY v FETCH FIRST 2 ROWS WITH TIES;
-- begin-expected
-- columns: id | v
-- row: 1 | 10
-- row: 2 | 20
-- end-expected
SELECT id, v FROM zz_lt ORDER BY v, id FETCH FIRST 2 ROWS WITH TIES;

-- ============================================================================
-- The count itself is read as a count
-- ============================================================================

-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- row: 20
-- row: 30
-- end-expected
SELECT v FROM zz_lt ORDER BY v LIMIT ALL;
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- row: 20
-- row: 30
-- end-expected
SELECT v FROM zz_lt ORDER BY v LIMIT NULL;
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- row: 20
-- row: 30
-- end-expected
SELECT v FROM zz_lt ORDER BY v OFFSET NULL;
-- begin-expected
-- columns: v
-- row: 10
-- end-expected
SELECT v FROM zz_lt ORDER BY v FETCH FIRST ROW ONLY;
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- end-expected
SELECT v FROM zz_lt ORDER BY v FETCH NEXT 2 ROWS ONLY;
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- end-expected
SELECT v FROM zz_lt ORDER BY v LIMIT '2';
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- end-expected
SELECT v FROM zz_lt ORDER BY v LIMIT (SELECT 2);
-- begin-expected-error
-- sqlstate: 2201W
-- end-expected-error
SELECT v FROM zz_lt ORDER BY v LIMIT -1;
-- begin-expected-error
-- sqlstate: 2201X
-- end-expected-error
SELECT v FROM zz_lt ORDER BY v OFFSET -1;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT v FROM zz_lt ORDER BY v LIMIT 'x';
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT v FROM zz_lt LIMIT 1 WITH TIES;

-- ============================================================================
-- A window offset is read on the row the answer is for
-- ============================================================================

-- begin-expected
-- columns: lag
-- row: NULL
-- row: NULL
-- row: NULL
-- end-expected
SELECT lag(v, v) OVER (ORDER BY v) FROM (VALUES (10), (20), (30)) t(v) ORDER BY 1;
-- begin-expected
-- columns: lead
-- row: NULL
-- row: NULL
-- row: NULL
-- end-expected
SELECT lead(v, v) OVER (ORDER BY v) FROM (VALUES (10), (20), (30)) t(v) ORDER BY 1;
-- begin-expected
-- columns: id | lag
-- row: 1 | NULL
-- row: 2 | NULL
-- row: 3 | NULL
-- row: 4 | NULL
-- end-expected
SELECT id, lag(v, id) OVER (ORDER BY id) FROM zz_lt ORDER BY id;
-- begin-expected
-- columns: id | lead
-- row: 1 | 20
-- row: 2 | 30
-- row: 3 | -1
-- row: 4 | -1
-- end-expected
SELECT id, lead(v, id, -1) OVER (ORDER BY id) FROM zz_lt ORDER BY id;

-- Its own value decides nothing about which function was called.
-- begin-expected
-- columns: lag
-- row: 10
-- row: 20
-- row: 20
-- row: 30
-- end-expected
SELECT lag(v, 0) OVER (ORDER BY v) FROM zz_lt ORDER BY 1;
-- begin-expected
-- columns: lag
-- row: 20
-- row: 20
-- row: 30
-- row: NULL
-- end-expected
SELECT lag(v, -1) OVER (ORDER BY v) FROM zz_lt ORDER BY 1;
-- begin-expected
-- columns: lag
-- row: NULL
-- row: NULL
-- row: NULL
-- row: NULL
-- end-expected
SELECT lag(v, NULL) OVER (ORDER BY v) FROM zz_lt ORDER BY 1;
-- begin-expected
-- columns: lag
-- row: -1
-- row: 10
-- row: 20
-- row: 20
-- end-expected
SELECT lag(v, 1, -1) OVER (ORDER BY v) FROM zz_lt ORDER BY 1;

-- ============================================================================
-- A frame offset is a count the type can hold
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v ROWS 99999999999999999999 PRECEDING) FROM zz_lt ORDER BY 1;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v ROWS 9223372036854775808 PRECEDING) FROM zz_lt ORDER BY 1;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 99999999999999999999 FOLLOWING) FROM zz_lt ORDER BY 1;
-- begin-expected
-- columns: sum
-- row: 10
-- row: 30
-- row: 50
-- row: 80
-- end-expected
SELECT sum(v) OVER (ORDER BY v ROWS 9223372036854775807 PRECEDING) FROM zz_lt ORDER BY 1;
-- begin-expected
-- columns: sum
-- row: 10
-- row: 30
-- row: 50
-- row: 70
-- end-expected
SELECT sum(v) OVER (ORDER BY v ROWS 2 PRECEDING) FROM zz_lt ORDER BY 1;
-- begin-expected-error
-- sqlstate: 22013
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v ROWS -1 PRECEDING) FROM zz_lt ORDER BY 1;

-- teardown
DROP TABLE zz_lt;
