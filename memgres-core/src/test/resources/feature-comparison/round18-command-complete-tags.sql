-- ============================================================================
-- Feature Comparison: Round 18 — CommandComplete tags
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
--
-- Row-count surface: pgjdbc reports CommandComplete row-count via
-- Statement.executeUpdate() return value. SQL-level counterpart is the count
-- visible in the resulting table.

-- ============================================================================
-- SECTION AE1: MERGE row count
-- ============================================================================

DROP TABLE IF EXISTS r18_mc_src;
DROP TABLE IF EXISTS r18_mc_tgt;
CREATE TABLE r18_mc_tgt(id int PRIMARY KEY, v int);
CREATE TABLE r18_mc_src(id int PRIMARY KEY, v int);
INSERT INTO r18_mc_tgt VALUES (1, 10);
INSERT INTO r18_mc_src VALUES (1, 11), (2, 22);

MERGE INTO r18_mc_tgt t USING r18_mc_src s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- 1. Target now has 2 rows total (1 updated, 1 inserted)
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n FROM r18_mc_tgt;

-- ============================================================================
-- SECTION AE2: SELECT INTO row count
-- ============================================================================

DROP TABLE IF EXISTS r18_si_src;
DROP TABLE IF EXISTS r18_si_dst;
CREATE TABLE r18_si_src(a int);
INSERT INTO r18_si_src VALUES (1),(2),(3);

SELECT * INTO r18_si_dst FROM r18_si_src;

-- 2. Destination has 3 rows
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::int AS n FROM r18_si_dst;

-- ============================================================================
-- SECTION AE3: CREATE TABLE AS row count
-- ============================================================================

DROP TABLE IF EXISTS r18_ct_src;
DROP TABLE IF EXISTS r18_ct_dst;
CREATE TABLE r18_ct_src(a int);
INSERT INTO r18_ct_src VALUES (1),(2),(3),(4);

CREATE TABLE r18_ct_dst AS SELECT * FROM r18_ct_src;

-- 3. New table has 4 rows
-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::int AS n FROM r18_ct_dst;
