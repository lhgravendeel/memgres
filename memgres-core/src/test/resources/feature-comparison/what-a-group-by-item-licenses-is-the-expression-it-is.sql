-- ============================================================================
-- What a GROUP BY item licenses is the expression it is
-- Grouping by an expression makes that expression available and nothing else, so what the
-- select list may say is decided by comparing whole expressions. Compared as canonicalised
-- text the comparison answered for the wrong things: every literal leaf was lowercased, so
-- 'a' and 'A' were the same value; a node holding its parts privately had no parts to compare,
-- so one subscript was every subscript; a ROW(...) was flattened into its members as though it
-- had been written as a bare parenthesised list; and a cast written float was not recognised
-- as the type the column already has.
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS gbl_t;
CREATE TABLE gbl_t (a int, b int, d double precision, r real, s text, arr int[], j jsonb);
INSERT INTO gbl_t VALUES (1, 2, 1.5, 1.5, 'x', '{10,20}', '{"k":1,"m":2}'), (3, 4, 2.5, 2.5, 'y', '{30,40}', '{"k":3,"m":4}');

-- ============================================================================
-- a literal says what it spells, and 'a' is not 'A'
-- ============================================================================
-- The grouped expression and the selected one differ in the value they read, so the column
-- under them is grouped by neither.
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT s || 'A' FROM gbl_t GROUP BY s || 'a' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT s = 'A' FROM gbl_t GROUP BY s = 'a' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT s LIKE 'A%' FROM gbl_t GROUP BY s LIKE 'a%' ORDER BY 1;
-- begin-expected
-- columns: x
-- row: xA
-- row: yA
-- end-expected
SELECT s || 'A' AS x FROM gbl_t GROUP BY s || 'A' ORDER BY 1;
-- begin-expected
-- columns: x
-- row: f
-- end-expected
SELECT s LIKE 'a%' AS x FROM gbl_t GROUP BY s LIKE 'a%' ORDER BY 1;
-- A name is read without regard to case, so the function and the type may be spelled either way.
-- begin-expected
-- columns: x
-- row: X
-- row: Y
-- end-expected
SELECT upper(s) AS x FROM gbl_t GROUP BY UPPER(s) ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1
-- row: 3
-- end-expected
SELECT a::text AS x FROM gbl_t GROUP BY a::TEXT ORDER BY 1;
-- ============================================================================
-- one subscript is not every subscript
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT arr[1] FROM gbl_t GROUP BY arr[2] ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 10
-- row: 30
-- end-expected
SELECT arr[1] AS x FROM gbl_t GROUP BY arr[1] ORDER BY 1;
-- begin-expected
-- columns: x
-- row: {10,20}
-- row: {30,40}
-- end-expected
SELECT arr[1:2] AS x FROM gbl_t GROUP BY arr[1:2] ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT arr[1:2] FROM gbl_t GROUP BY arr[1:1] ORDER BY 1;
-- An operator applied to different arguments is a different expression too.
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT j -> 'k' FROM gbl_t GROUP BY j -> 'm' ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1
-- row: 3
-- end-expected
SELECT j ->> 'k' AS x FROM gbl_t GROUP BY j ->> 'k' ORDER BY 1;
-- ============================================================================
-- a parenthesised list is a list of items, ROW(...) is one expression
-- ============================================================================
-- Written as a list, each member is an item of its own and is grouped by.
-- begin-expected
-- columns: a | b
-- row: 1 | 2
-- row: 3 | 4
-- end-expected
SELECT a, b FROM gbl_t GROUP BY (a, b) ORDER BY 1;
-- begin-expected
-- columns: a | b
-- row: 1 | 2
-- row: 3 | 4
-- end-expected
SELECT a, b FROM gbl_t GROUP BY ((a, b)) ORDER BY 1;
-- begin-expected
-- columns: a
-- row: 1
-- row: 3
-- end-expected
SELECT a FROM gbl_t GROUP BY (a, a) ORDER BY 1;
-- begin-expected
-- columns: a | b
-- row: 1 | 2
-- row: 3 | 4
-- end-expected
SELECT a, b FROM gbl_t GROUP BY GROUPING SETS ((a, b)) ORDER BY 1;
-- Written with the keyword, it is one expression of row type and licenses that row alone.
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT a, b FROM gbl_t GROUP BY ROW(a, b) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT a FROM gbl_t GROUP BY ROW(a) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT a, b FROM gbl_t GROUP BY ROLLUP (ROW(a, b)) ORDER BY 1;
-- begin-expected
-- columns: a | b
-- row: 1 | 2
-- row: 3 | 4
-- end-expected
SELECT a, b FROM gbl_t GROUP BY ROW(a, b), a, b ORDER BY 1;
-- Having grouped by the row, the row itself may be selected under either spelling.
-- begin-expected
-- columns: x
-- row: (1,2)
-- row: (3,4)
-- end-expected
SELECT ROW(a, b) AS x FROM gbl_t GROUP BY ROW(a, b) ORDER BY 1;
-- begin-expected
-- columns: x
-- row: (1,2)
-- row: (3,4)
-- end-expected
SELECT (a, b) AS x FROM gbl_t GROUP BY ROW(a, b) ORDER BY 1;
-- begin-expected
-- columns: x
-- row: (1,2)
-- row: (3,4)
-- end-expected
SELECT ROW(a, b) AS x FROM gbl_t GROUP BY (a, b) ORDER BY 1;
-- ============================================================================
-- a cast to the type the column already has is not part of the expression
-- ============================================================================
-- float names double precision, and float(p) names the type that many bits of precision asks for.
-- begin-expected
-- columns: x
-- row: 1.5
-- row: 2.5
-- end-expected
SELECT d AS x FROM gbl_t GROUP BY d::float ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1.5
-- row: 2.5
-- end-expected
SELECT d AS x FROM gbl_t GROUP BY d::float(53) ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1.5
-- row: 2.5
-- end-expected
SELECT d AS x FROM gbl_t GROUP BY d::float8 ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1.5
-- row: 2.5
-- end-expected
SELECT r AS x FROM gbl_t GROUP BY r::float(24) ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1.5
-- row: 2.5
-- end-expected
SELECT r AS x FROM gbl_t GROUP BY r::float4 ORDER BY 1;
-- A cast naming any other type is a conversion, and leaves the column ungrouped.
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT d FROM gbl_t GROUP BY d::float(24) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT r FROM gbl_t GROUP BY r::float ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT d FROM gbl_t GROUP BY d::numeric ORDER BY 1;
-- ============================================================================
-- what was already licensed still is
-- ============================================================================
-- begin-expected
-- columns: x | n
-- row: 1 | 1
-- row: 3 | 1
-- end-expected
SELECT a AS x, count(*) AS n FROM gbl_t GROUP BY a ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 3
-- row: 7
-- end-expected
SELECT a + b AS x FROM gbl_t GROUP BY a + b ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT a + 1 FROM gbl_t GROUP BY 1 + a ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1
-- row: 3
-- end-expected
SELECT coalesce(a, 0) AS x FROM gbl_t GROUP BY coalesce(a, 0) ORDER BY 1;
-- begin-expected
-- columns: x
-- row: p
-- row: q
-- end-expected
SELECT CASE WHEN a = 1 THEN 'p' ELSE 'q' END AS x FROM gbl_t GROUP BY CASE WHEN a = 1 THEN 'p' ELSE 'q' END ORDER BY 1;
-- begin-expected
-- columns: x
-- row: {1}
-- row: {3}
-- end-expected
SELECT ARRAY[a] AS x FROM gbl_t GROUP BY ARRAY[a] ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT ARRAY[a] FROM gbl_t GROUP BY ARRAY[b] ORDER BY 1;
-- begin-expected
-- columns: x
-- row: 1
-- row: 3
-- end-expected
SELECT a AS x FROM gbl_t GROUP BY 1 ORDER BY 1;
-- begin-expected
-- columns: q
-- row: 1
-- row: 3
-- end-expected
SELECT a AS q FROM gbl_t GROUP BY q ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT b FROM gbl_t GROUP BY a ORDER BY 1;
-- begin-expected
-- columns: x | y
-- row: 1 | 2
-- row: 1 | NULL
-- row: 3 | 4
-- row: 3 | NULL
-- row: NULL | 2
-- row: NULL | 4
-- row: NULL | NULL
-- end-expected
SELECT a AS x, b AS y FROM gbl_t GROUP BY CUBE (a, b) ORDER BY 1, 2;
-- begin-expected
-- columns: x
-- row: 1
-- row: 3
-- end-expected
SELECT a AS x FROM gbl_t GROUP BY a HAVING count(*) > 0 ORDER BY 1;

-- teardown
DROP TABLE IF EXISTS gbl_t;
