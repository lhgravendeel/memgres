-- ============================================================================
-- Feature Comparison: Round 15 — Index variant options
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_iv CASCADE;
CREATE SCHEMA r15_iv;
SET search_path = r15_iv, public;

-- ============================================================================
-- SECTION A: INCLUDE covering columns
-- ============================================================================

CREATE TABLE r15_ix_inc (a int, b int, c text);
CREATE INDEX r15_ix_inc_idx ON r15_ix_inc (a) INCLUDE (b, c);

-- 1. indexdef contains INCLUDE
SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_inc_idx';

-- 2. indnatts=3, indnkeyatts=1
CREATE TABLE r15_ix_i2 (a int, b int, c int);
CREATE INDEX r15_ix_i2_idx ON r15_ix_i2 (a) INCLUDE (b, c);

-- begin-expected
-- columns: nat, nka
-- row: 3, 1
-- end-expected
SELECT indnatts::int AS nat, indnkeyatts::int AS nka FROM pg_index i
  JOIN pg_class c ON i.indexrelid = c.oid
  WHERE c.relname='r15_ix_i2_idx';

-- ============================================================================
-- SECTION B: NULLS NOT DISTINCT (PG 15+)
-- ============================================================================

CREATE TABLE r15_nnd (v int);
CREATE UNIQUE INDEX r15_nnd_idx ON r15_nnd (v) NULLS NOT DISTINCT;
INSERT INTO r15_nnd VALUES (NULL);

-- 3. NULLS NOT DISTINCT rejects duplicate nulls
-- begin-expected-error
-- message-like: unique
-- end-expected-error
INSERT INTO r15_nnd VALUES (NULL);

CREATE TABLE r15_nnd2 (v int);
CREATE UNIQUE INDEX r15_nnd2_idx ON r15_nnd2 (v) NULLS NOT DISTINCT;

-- 4. indexdef contains NULLS NOT DISTINCT
SELECT indexdef FROM pg_indexes WHERE indexname='r15_nnd2_idx';

-- ============================================================================
-- SECTION C: DESC / NULLS FIRST / NULLS LAST
-- ============================================================================

CREATE TABLE r15_ix_o (v int);
CREATE INDEX r15_ix_o_idx ON r15_ix_o (v DESC NULLS FIRST);

-- 5. DESC NULLS FIRST preserved
SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_o_idx';

-- ============================================================================
-- SECTION D: Opclass preservation
-- ============================================================================

CREATE TABLE r15_ix_oc (v text);
CREATE INDEX r15_ix_oc_idx ON r15_ix_oc (v text_pattern_ops);

-- 6. opclass text_pattern_ops preserved
SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_oc_idx';

-- ============================================================================
-- SECTION E: CONCURRENTLY
-- ============================================================================

CREATE TABLE r15_ix_c (id int);
CREATE INDEX CONCURRENTLY r15_ix_c_idx ON r15_ix_c (id);

-- 7. Index created
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_indexes WHERE indexname='r15_ix_c_idx';

CREATE TABLE r15_ix_dc (id int);
CREATE INDEX r15_ix_dc_idx ON r15_ix_dc (id);
DROP INDEX CONCURRENTLY r15_ix_dc_idx;

-- 8. Index dropped
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_indexes WHERE indexname='r15_ix_dc_idx';

-- ============================================================================
-- SECTION F: ALTER INDEX ATTACH PARTITION
-- ============================================================================

CREATE TABLE r15_pp (id int, region int) PARTITION BY LIST (region);
CREATE TABLE r15_pp_1 PARTITION OF r15_pp FOR VALUES IN (1);
CREATE INDEX r15_pp_parent_idx ON r15_pp (id);
CREATE INDEX r15_pp_1_idx ON r15_pp_1 (id);
ALTER INDEX r15_pp_parent_idx ATTACH PARTITION r15_pp_1_idx;

-- 9. child→parent inheritance wired (auto-created child from CREATE INDEX on partitioned table)
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_index i
  JOIN pg_inherits h ON h.inhrelid = i.indexrelid
  JOIN pg_class pc ON h.inhparent = pc.oid
  WHERE pc.relname='r15_pp_parent_idx';

-- ============================================================================
-- SECTION G: Expression index
-- ============================================================================

CREATE TABLE r15_ix_e (v text);
CREATE INDEX r15_ix_e_idx ON r15_ix_e (lower(v));

-- 10. indexdef contains lower()
SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_e_idx';

-- ============================================================================
-- SECTION H: Partial index
-- ============================================================================

CREATE TABLE r15_ix_p (id int, active bool);
CREATE INDEX r15_ix_p_idx ON r15_ix_p (id) WHERE active;

-- 11. indexdef contains WHERE
SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_p_idx';

-- ============================================================================
-- SECTION I: USING method
-- ============================================================================

CREATE TABLE r15_ix_m (id int);
CREATE INDEX r15_ix_m_idx ON r15_ix_m USING hash (id);

-- 12. indexdef has USING hash
SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_m_idx';
