-- ============================================================================
-- Feature Comparison: PREPARE parameter inference in all statement positions
--                     + FETCH/MOVE ABSOLUTE past-the-end repositioning
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers:
--   * $N parameters in JOIN ON conditions, CTE bodies (incl. recursive
--     branches), HAVING, ORDER BY, LIMIT/OFFSET, VALUES-in-FROM, set-operation
--     branches, window expressions and ON CONFLICT SET/WHERE clauses
--   * wrong parameter count error unchanged
--   * FETCH ABSOLUTE beyond either end repositions the cursor (after-last /
--     before-first) even though it returns 0 rows; MOVE shares the behavior
--   * FETCH FIRST/LAST on an empty cursor
-- NOTIFY dedup timing cannot be asserted in this single-connection format;
-- it is covered by unit tests (PrepareNotifyFetchTest).
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS pf_a CASCADE;
DROP TABLE IF EXISTS pf_b CASCADE;
DROP TABLE IF EXISTS pf_conf CASCADE;
CREATE TABLE pf_a (id int PRIMARY KEY, v text);
CREATE TABLE pf_b (id int PRIMARY KEY, a_id int);
CREATE TABLE pf_conf (id int PRIMARY KEY, v text);
INSERT INTO pf_a VALUES (1, 'one'), (2, 'two'), (3, 'three');
INSERT INTO pf_b VALUES (10, 1), (20, 2), (30, 3), (40, 3);
INSERT INTO pf_conf VALUES (1, 'a');

-- ============================================================================
-- 1. Parameter in JOIN ON condition
-- ============================================================================

PREPARE pf_pj AS SELECT a.id FROM pf_a a JOIN pf_b b ON b.id = $1 AND b.a_id = a.id;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
EXECUTE pf_pj(20);

-- 1b. Wrong parameter count still errors
-- begin-expected-error
-- sqlstate: 42601
-- message-like: wrong number of parameters for prepared statement
-- end-expected-error
EXECUTE pf_pj;

-- ============================================================================
-- 2. Parameter in CTE body
-- ============================================================================

PREPARE pf_pc AS WITH w AS (SELECT $1::int AS a) SELECT a FROM w;

-- begin-expected
-- columns: a
-- row: 42
-- end-expected
EXECUTE pf_pc(42);

-- ============================================================================
-- 3. Parameters in both branches of a recursive CTE
-- ============================================================================

PREPARE pf_pr AS WITH RECURSIVE r(n) AS (
    SELECT $1::int
    UNION ALL
    SELECT n + 1 FROM r WHERE n < $2::int
) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 2
-- row: 3
-- row: 4
-- end-expected
EXECUTE pf_pr(2, 4);

-- ============================================================================
-- 4. Parameter in HAVING
-- ============================================================================

PREPARE pf_ph AS SELECT a_id FROM pf_b GROUP BY a_id HAVING count(*) > $1 ORDER BY a_id;

-- begin-expected
-- columns: a_id
-- row: 3
-- end-expected
EXECUTE pf_ph(1);

-- ============================================================================
-- 5. Parameter in ORDER BY
-- ============================================================================

PREPARE pf_po AS SELECT id FROM pf_a ORDER BY id * $1::int;

-- begin-expected
-- columns: id
-- row: 3
-- row: 2
-- row: 1
-- end-expected
EXECUTE pf_po(-1);

-- ============================================================================
-- 6. Parameters in LIMIT / OFFSET
-- ============================================================================

PREPARE pf_pl AS SELECT id FROM pf_a ORDER BY id LIMIT $1 OFFSET $2;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
EXECUTE pf_pl(1, 1);

-- ============================================================================
-- 7. Parameters in VALUES used as a FROM item
-- ============================================================================

PREPARE pf_pv AS SELECT x FROM (VALUES ($1::int), ($2::int)) AS v(x) ORDER BY x;

-- begin-expected
-- columns: x
-- row: 5
-- row: 9
-- end-expected
EXECUTE pf_pv(9, 5);

-- ============================================================================
-- 8. Parameters in both branches of a UNION
-- ============================================================================

PREPARE pf_pu AS SELECT $1::int AS x UNION ALL SELECT $2::int ORDER BY x;

-- begin-expected
-- columns: x
-- row: 1
-- row: 7
-- end-expected
EXECUTE pf_pu(7, 1);

-- ============================================================================
-- 9. Parameter inside a window expression
-- ============================================================================

PREPARE pf_pw AS SELECT sum(id + $1::int) OVER () AS s FROM pf_a LIMIT 1;

-- begin-expected
-- columns: s
-- row: 36
-- end-expected
EXECUTE pf_pw(10);

-- ============================================================================
-- 10. Parameters in ON CONFLICT DO UPDATE SET and its WHERE clause
-- ============================================================================

PREPARE pf_pcu AS INSERT INTO pf_conf VALUES ($1, $2)
    ON CONFLICT (id) DO UPDATE SET v = $3 WHERE pf_conf.v <> $3;

EXECUTE pf_pcu(1, 'ignored', 'updated');

-- begin-expected
-- columns: v
-- row: updated
-- end-expected
SELECT v FROM pf_conf WHERE id = 1;

-- ============================================================================
-- 11. FETCH ABSOLUTE past the end repositions after the last row
-- ============================================================================

BEGIN;

DECLARE pf_c1 SCROLL CURSOR FOR SELECT id FROM pf_a ORDER BY id;

-- 11a. Past-end ABSOLUTE returns no rows
-- begin-expected
-- columns: id
-- end-expected
FETCH ABSOLUTE 100 FROM pf_c1;

-- 11b. ...but the cursor now sits after the last row: PRIOR returns the last row
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH PRIOR FROM pf_c1;

-- 11c. Negative past-start ABSOLUTE returns no rows
-- begin-expected
-- columns: id
-- end-expected
FETCH ABSOLUTE -100 FROM pf_c1;

-- 11d. ...and the cursor now sits before the first row: NEXT returns the first row
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM pf_c1;

-- 11e. In-range ABSOLUTE still works after clamping
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH ABSOLUTE 2 FROM pf_c1;

CLOSE pf_c1;

-- ============================================================================
-- 12. MOVE ABSOLUTE shares the repositioning behavior (returns no rows)
-- ============================================================================

DECLARE pf_c2 SCROLL CURSOR FOR SELECT id FROM pf_a ORDER BY id;

MOVE ABSOLUTE 100 IN pf_c2;

-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH PRIOR FROM pf_c2;

MOVE ABSOLUTE -100 IN pf_c2;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM pf_c2;

CLOSE pf_c2;

-- ============================================================================
-- 13. FETCH FIRST/LAST/ABSOLUTE on an empty cursor
-- ============================================================================

DECLARE pf_ce SCROLL CURSOR FOR SELECT id FROM pf_a WHERE id > 999;

-- begin-expected
-- columns: id
-- end-expected
FETCH LAST FROM pf_ce;

-- begin-expected
-- columns: id
-- end-expected
FETCH NEXT FROM pf_ce;

-- begin-expected
-- columns: id
-- end-expected
FETCH FIRST FROM pf_ce;

-- begin-expected
-- columns: id
-- end-expected
FETCH PRIOR FROM pf_ce;

-- begin-expected
-- columns: id
-- end-expected
FETCH ABSOLUTE 5 FROM pf_ce;

-- begin-expected
-- columns: id
-- end-expected
FETCH ABSOLUTE -5 FROM pf_ce;

CLOSE pf_ce;
COMMIT;

-- ============================================================================
-- Cleanup
-- ============================================================================

DEALLOCATE pf_pj;
DEALLOCATE pf_pc;
DEALLOCATE pf_pr;
DEALLOCATE pf_ph;
DEALLOCATE pf_po;
DEALLOCATE pf_pl;
DEALLOCATE pf_pv;
DEALLOCATE pf_pu;
DEALLOCATE pf_pw;
DEALLOCATE pf_pcu;
DROP TABLE IF EXISTS pf_a CASCADE;
DROP TABLE IF EXISTS pf_b CASCADE;
DROP TABLE IF EXISTS pf_conf CASCADE;
