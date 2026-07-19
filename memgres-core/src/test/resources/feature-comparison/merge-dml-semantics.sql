-- SQL verification: MERGE guards, DML CTEs, RETURNING sources (C3, C5, H1, M6)

-- =============================================================================
-- C3: MERGE raises 21000 when duplicate source rows match one target row
-- =============================================================================
CREATE TABLE t_c3 (id int PRIMARY KEY, val text);
INSERT INTO t_c3 VALUES (1, 'a'), (2, 'b');
CREATE TABLE s_c3 (id int, val text);
INSERT INTO s_c3 VALUES (2, 'x'), (2, 'y');

-- PG: ERROR 21000 "MERGE command cannot affect row a second time"
MERGE INTO t_c3 t USING s_c3 s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET val = s.val;

DROP TABLE t_c3, s_c3;

-- =============================================================================
-- C5: Unreferenced data-modifying CTEs are executed
-- =============================================================================
CREATE TABLE t_c5 (id int, val text);
WITH ins AS (INSERT INTO t_c5 VALUES (1, 'inserted') RETURNING id)
SELECT 42;
SELECT * FROM t_c5;
-- expected: (1, 'inserted')

DROP TABLE t_c5;

-- =============================================================================
-- H1: RETURNING can reference FROM/USING/source tables
-- =============================================================================
CREATE TABLE t_h1 (id int PRIMARY KEY, val text);
CREATE TABLE s_h1 (id int PRIMARY KEY, sval text);
INSERT INTO t_h1 VALUES (1, 'a'), (2, 'b');
INSERT INTO s_h1 VALUES (1, 'x'), (2, 'y');

-- UPDATE...FROM RETURNING source column
UPDATE t_h1 t SET val = s.sval FROM s_h1 s WHERE t.id = s.id RETURNING t.id, s.sval;
-- expected: (1, 'x'), (2, 'y')

-- DELETE...USING RETURNING source column
DELETE FROM t_h1 t USING s_h1 s WHERE t.id = s.id RETURNING t.id, s.sval;
-- expected: (1, 'x'), (2, 'y')

DROP TABLE t_h1, s_h1;

-- =============================================================================
-- M6: MERGE ... WHEN NOT MATCHED THEN INSERT DEFAULT VALUES
-- =============================================================================
CREATE TABLE t_m6 (id serial PRIMARY KEY, val text DEFAULT 'default_val');
CREATE TABLE s_m6 (id int);
INSERT INTO s_m6 VALUES (999);

MERGE INTO t_m6 t USING s_m6 s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;
SELECT * FROM t_m6;
-- expected: (1, 'default_val')

DROP TABLE t_m6, s_m6;
