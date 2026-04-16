-- ============================================================================
-- Feature Comparison: Round 15 — Extended statistics (CREATE STATISTICS)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_es CASCADE;
CREATE SCHEMA r15_es;
SET search_path = r15_es, public;

-- ============================================================================
-- SECTION A: CREATE STATISTICS kinds
-- ============================================================================

CREATE TABLE r15_st_dep (a int, b int, c int);
CREATE STATISTICS r15_stat_dep (dependencies) ON a, b FROM r15_st_dep;

-- 1. dependencies
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_dep';

CREATE TABLE r15_st_nd (a int, b int);
CREATE STATISTICS r15_stat_nd (ndistinct) ON a, b FROM r15_st_nd;

-- 2. ndistinct
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_nd';

CREATE TABLE r15_st_mcv (a int, b int);
CREATE STATISTICS r15_stat_mcv (mcv) ON a, b FROM r15_st_mcv;

-- 3. mcv
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_mcv';

CREATE TABLE r15_st_all (a int, b int);
CREATE STATISTICS r15_stat_all (ndistinct, dependencies, mcv) ON a, b FROM r15_st_all;

-- 4. all kinds
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_all';

-- ============================================================================
-- SECTION B: CREATE STATISTICS on expressions (PG 14+)
-- ============================================================================

CREATE TABLE r15_st_expr (a int, b int);
CREATE STATISTICS r15_stat_expr ON (a + b) FROM r15_st_expr;

-- 5. Expression stat
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_expr';

CREATE TABLE r15_st_me (a int, b int, c int);
CREATE STATISTICS r15_stat_me ON (a + b), (b * c), c FROM r15_st_me;

-- 6. Multi expressions
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_me';

-- ============================================================================
-- SECTION C: pg_statistic_ext_data populated by ANALYZE
-- ============================================================================

CREATE TABLE r15_st_data (a int, b int);
INSERT INTO r15_st_data SELECT g%10, g%3 FROM generate_series(0,99) g;
CREATE STATISTICS r15_stat_data (ndistinct) ON a, b FROM r15_st_data;
ANALYZE r15_st_data;

-- 7. pg_statistic_ext_data has row
SELECT count(*)::int AS c FROM pg_statistic_ext_data e
  JOIN pg_statistic_ext s ON e.stxoid = s.oid
  WHERE s.stxname='r15_stat_data';

-- ============================================================================
-- SECTION D: ALTER STATISTICS
-- ============================================================================

CREATE TABLE r15_st_alt (a int, b int);
CREATE STATISTICS r15_stat_alt (ndistinct) ON a, b FROM r15_st_alt;
ALTER STATISTICS r15_stat_alt SET STATISTICS 1000;

-- 8. stxstattarget updated
-- begin-expected
-- columns: t
-- row: 1000
-- end-expected
SELECT stxstattarget::int AS t FROM pg_statistic_ext WHERE stxname='r15_stat_alt';

CREATE TABLE r15_st_ren (a int, b int);
CREATE STATISTICS r15_stat_ren1 (ndistinct) ON a, b FROM r15_st_ren;
ALTER STATISTICS r15_stat_ren1 RENAME TO r15_stat_ren2;

-- 9. RENAME
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_ren2';

-- ============================================================================
-- SECTION E: DROP STATISTICS
-- ============================================================================

CREATE TABLE r15_st_drop (a int, b int);
CREATE STATISTICS r15_stat_drop (ndistinct) ON a, b FROM r15_st_drop;
DROP STATISTICS r15_stat_drop;

-- 10. Dropped
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_statistic_ext WHERE stxname='r15_stat_drop';

-- 11. IF EXISTS
DROP STATISTICS IF EXISTS r15_stat_nonexistent;

-- ============================================================================
-- SECTION F: pg_stats_ext view
-- ============================================================================

-- 12. pg_stats_ext shape
SELECT schemaname, tablename, statistics_name, attnames, kinds, exprs
  FROM pg_stats_ext LIMIT 1;
