-- ============================================================================
-- An arm of a set operation is the query it was written as
--
-- A WITH clause written in front of a set operation belongs to the whole
-- operation: its items are in scope on both arms. The arm that carries the text
-- of the clause is therefore run once more with the items taken off, so that it
-- does not declare them a second time -- and that is where an arm stopped being
-- the query it was written as. Written out again through a shorter constructor it
-- came back without its GROUPING SETS, without its WINDOW definitions, and without
-- what its HAVING said, and the operation answered over a query nobody had asked
-- for.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE soa_t (g int, v int);
INSERT INTO soa_t VALUES (1, 10), (1, 20), (2, 30);

-- ============================================================================
-- The grouping an arm was written with is the grouping it is run with
-- ============================================================================

-- GROUPING SETS asks for one row per set named, and the empty set is the
-- grand total; an arm that lost them grouped by g alone.
-- begin-expected
-- columns: g | sum
-- row: 1 | 30
-- row: 2 | 30
-- row: 9 | 9
-- row: NULL | 60
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, sum(v) FROM c GROUP BY GROUPING SETS ((g), ()) UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST;

-- ROLLUP and CUBE are the same question written shorter.
-- begin-expected
-- columns: g | count
-- row: 1 | 2
-- row: 2 | 1
-- row: 9 | 9
-- row: NULL | 3
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, count(*) FROM c GROUP BY ROLLUP(g) UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST;

-- begin-expected
-- columns: g | sum
-- row: 1 | 10
-- row: 1 | 20
-- row: 1 | 30
-- row: 2 | 30
-- row: 2 | 30
-- row: 9 | 9
-- row: NULL | 10
-- row: NULL | 20
-- row: NULL | 30
-- row: NULL | 60
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, sum(v) FROM c GROUP BY CUBE(g, v) UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST, 2 NULLS LAST;

-- GROUPING says which of the sets a row came from, so it answers only where
-- the sets survived.
-- begin-expected
-- columns: g | grouping
-- row: 1 | 0
-- row: 2 | 0
-- row: 9 | 9
-- row: NULL | 1
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, grouping(g) FROM c GROUP BY GROUPING SETS ((g), ()) UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST;

-- What HAVING says is part of the arm too.
-- begin-expected
-- columns: g | count
-- row: 1 | 2
-- row: 9 | 9
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, count(*) FROM c GROUP BY g HAVING count(*) > 1 UNION ALL SELECT 9, 9 ORDER BY 1;

-- begin-expected
-- columns: g | count
-- row: 1 | 2
-- row: 9 | 9
-- row: NULL | 3
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, count(*) FROM c GROUP BY CUBE(g) HAVING count(*) > 1 UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST;

-- ============================================================================
-- A window an arm named is a window the arm still has
-- ============================================================================

-- begin-expected
-- columns: g | sum
-- row: 1 | 30
-- row: 1 | 30
-- row: 2 | 30
-- row: 9 | 9
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, sum(v) OVER w FROM c WINDOW w AS (PARTITION BY g) UNION ALL SELECT 9, 9 ORDER BY 1, 2;

-- begin-expected
-- columns: g | rank
-- row: 1 | 1
-- row: 1 | 2
-- row: 2 | 1
-- row: 9 | 9
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, rank() OVER w FROM c WINDOW w AS (PARTITION BY g ORDER BY v) UNION ALL SELECT 9, 9 ORDER BY 1, 2;

-- ============================================================================
-- DISTINCT, and every operator the two arms may be joined by
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- row: 9
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT DISTINCT g FROM c UNION ALL SELECT 9 ORDER BY 1;

-- begin-expected
-- columns: g | sum
-- row: 1 | 30
-- row: 2 | 30
-- row: NULL | 60
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, sum(v) FROM c GROUP BY GROUPING SETS ((g), ()) INTERSECT SELECT g, sum(v) FROM soa_t GROUP BY GROUPING SETS ((g), ()) ORDER BY 1 NULLS LAST;

-- begin-expected
-- columns: g | sum
-- row: 2 | 30
-- row: NULL | 60
-- end-expected
WITH c AS (SELECT g, v FROM soa_t) SELECT g, sum(v) FROM c GROUP BY GROUPING SETS ((g), ()) EXCEPT SELECT 1, 30 ORDER BY 1 NULLS LAST;

-- The items are in scope on the arm that did not declare them, and what the
-- whole operation is limited to is asked of the whole operation.
-- begin-expected
-- columns: g
-- row: 1
-- row: 1
-- row: 1
-- row: 2
-- end-expected
WITH c AS (SELECT g FROM soa_t) SELECT g FROM c UNION ALL SELECT g FROM c ORDER BY 1 LIMIT 4 OFFSET 1;

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
WITH c AS (SELECT g FROM soa_t) SELECT g FROM c UNION SELECT g FROM c ORDER BY 1;

-- teardown
DROP TABLE soa_t;
