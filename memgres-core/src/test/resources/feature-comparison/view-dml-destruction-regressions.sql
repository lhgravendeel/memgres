-- ============================================================================
-- Feature Comparison: view/DML regressions that destroy or lose data
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Five paths where a statement that PostgreSQL rejects or reorders silently
-- removed or corrupted rows: view auto-updatability judged from the shape of a
-- target expression, data-modifying CTE ordering, an inner view's WITH CHECK
-- OPTION, and MERGE's "affect a row a second time" guard.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS vdr_t CASCADE;
DROP TABLE IF EXISTS vdr_cte CASCADE;
DROP TABLE IF EXISTS vdr_chk CASCADE;
DROP TABLE IF EXISTS vdr_tgt CASCADE;
DROP TABLE IF EXISTS vdr_src CASCADE;

-- a primary key gives a replica identity, so UPDATE works under a publication
CREATE TABLE vdr_t (id int PRIMARY KEY, val int);
INSERT INTO vdr_t VALUES (1,10),(2,20);

CREATE VIEW vdr_agg AS SELECT sum(val)+1 AS s FROM vdr_t;
CREATE VIEW vdr_win AS SELECT id, row_number() OVER () AS rn FROM vdr_t;
CREATE VIEW vdr_dist AS SELECT DISTINCT id FROM vdr_t;
CREATE VIEW vdr_grp AS SELECT id, count(*) AS c FROM vdr_t GROUP BY id;
CREATE VIEW vdr_expr AS SELECT id, val*2 AS dbl FROM vdr_t;

-- ============================================================================
-- 1. A view whose target merely contains an aggregate or window call is read-only
-- ============================================================================

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view
-- end-expected-error
DELETE FROM vdr_agg;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view
-- end-expected-error
DELETE FROM vdr_win;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view
-- end-expected-error
DELETE FROM vdr_dist;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view
-- end-expected-error
DELETE FROM vdr_grp;

-- The base table must still hold both rows
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM vdr_t;

-- ============================================================================
-- 2. An expression column cannot be assigned; a plain column still can
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot update column
-- end-expected-error
UPDATE vdr_expr SET dbl = 5;

-- begin-expected
-- columns: id | val
-- row: 1, 10
-- row: 2, 20
-- end-expected
SELECT id, val FROM vdr_t ORDER BY id;

UPDATE vdr_expr SET id = 5 WHERE id = 1;

-- begin-expected
-- columns: id | val
-- row: 2, 20
-- row: 5, 10
-- end-expected
SELECT id, val FROM vdr_t ORDER BY id;

-- ============================================================================
-- 3. Data-modifying CTEs run before the main statement
-- ============================================================================

CREATE TABLE vdr_cte (a int PRIMARY KEY, b int);
INSERT INTO vdr_cte VALUES (1,1),(2,2);

WITH d AS (DELETE FROM vdr_cte RETURNING *) INSERT INTO vdr_cte VALUES (99,99);

-- begin-expected
-- columns: a | b
-- row: 99, 99
-- end-expected
SELECT a, b FROM vdr_cte ORDER BY a;

-- ============================================================================
-- 4. An inner view's WITH CHECK OPTION applies through an outer view
-- ============================================================================

CREATE TABLE vdr_chk (id int, val int);
CREATE VIEW vdr_chk_inner AS SELECT * FROM vdr_chk WHERE val > 0 WITH CHECK OPTION;
CREATE VIEW vdr_chk_outer AS SELECT * FROM vdr_chk_inner;

-- begin-expected-error
-- sqlstate: 44000
-- message-like: check option
-- end-expected-error
INSERT INTO vdr_chk_outer VALUES (1, -5);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM vdr_chk;

INSERT INTO vdr_chk_outer VALUES (2, 5);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM vdr_chk;

-- ============================================================================
-- 5. MERGE: DO NOTHING does not "affect" a row
-- ============================================================================

-- a primary key gives the table a replica identity, so UPDATE works under a publication
CREATE TABLE vdr_tgt (id int PRIMARY KEY, tag text, v int);
CREATE TABLE vdr_src (id int, tag text);
INSERT INTO vdr_tgt VALUES (1,'x',0);
INSERT INTO vdr_src VALUES (1,'a'),(1,'b');

MERGE INTO vdr_tgt t USING vdr_src s ON t.id = s.id
  WHEN MATCHED AND s.tag = 'a' THEN DO NOTHING
  WHEN MATCHED AND s.tag = 'b' THEN UPDATE SET v = 99;

-- begin-expected
-- columns: id | tag | v
-- row: 1, x, 99
-- end-expected
SELECT id, tag, v FROM vdr_tgt;

-- Two real modifications of one row are still an error
DROP TABLE IF EXISTS vdr_tgt2 CASCADE;
DROP TABLE IF EXISTS vdr_src2 CASCADE;
CREATE TABLE vdr_tgt2 (id int PRIMARY KEY, v int);
CREATE TABLE vdr_src2 (id int, v int);
INSERT INTO vdr_tgt2 VALUES (1,0);
INSERT INTO vdr_src2 VALUES (1,10),(1,20);

-- begin-expected-error
-- sqlstate: 21000
-- message-like: affect row a second time
-- end-expected-error
MERGE INTO vdr_tgt2 t USING vdr_src2 s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE vdr_src2 CASCADE;
DROP TABLE vdr_tgt2 CASCADE;
DROP TABLE vdr_src CASCADE;
DROP TABLE vdr_tgt CASCADE;
DROP TABLE vdr_chk CASCADE;
DROP TABLE vdr_cte CASCADE;
DROP TABLE vdr_t CASCADE;
