-- ============================================================================
-- Feature Comparison: Round 15 — COPY extra options
-- Target: PostgreSQL 18 vs Memgres
--
-- Note: COPY FROM STDIN requires a driver session so many tests are
-- expressed as Java only; the SQL here documents the syntax surface.
-- ============================================================================

DROP SCHEMA IF EXISTS r15_copy CASCADE;
CREATE SCHEMA r15_copy;
SET search_path = r15_copy, public;

-- ============================================================================
-- SECTION A: COPY FROM … WHERE (PG 12+)
-- ============================================================================

CREATE TABLE r15_cw (id int, v int);

-- 1. Syntax: COPY FROM … WITH (FORMAT csv) WHERE v > 10
-- Accepted as parse-only (actual data transfer is driver-specific).

-- ============================================================================
-- SECTION B: HEADER MATCH (PG 14+)
-- ============================================================================

CREATE TABLE r15_hm (id int, v int);

-- 2. HEADER MATCH syntax accepted

-- ============================================================================
-- SECTION C: FORCE_NULL / FORCE_NOT_NULL
-- ============================================================================

CREATE TABLE r15_fn (id int, v text);

-- 3. FORCE_NULL (v)

CREATE TABLE r15_fnn (id int, v text);

-- 4. FORCE_NOT_NULL (v)

-- ============================================================================
-- SECTION D: ON_ERROR (PG 17+)
-- ============================================================================

CREATE TABLE r15_oe_stop (id int, v int);

-- 5. ON_ERROR stop

CREATE TABLE r15_oe_ign (id int, v int);

-- 6. ON_ERROR ignore

-- ============================================================================
-- SECTION E: LOG_VERBOSITY (PG 17+)
-- ============================================================================

CREATE TABLE r15_lv (id int);

-- 7. LOG_VERBOSITY verbose

-- ============================================================================
-- SECTION F: DEFAULT (PG 17+)
-- ============================================================================

CREATE TABLE r15_cd (id int, v text DEFAULT 'DFLT');

-- 8. DEFAULT '\D'

-- ============================================================================
-- SECTION G: (removed — COPY PROGRAM is intentionally unsupported)
-- ============================================================================

-- ============================================================================
-- SECTION H: BINARY format
-- ============================================================================

CREATE TABLE r15_cb (id int);
INSERT INTO r15_cb VALUES (1),(2),(3);

-- 10. COPY r15_cb TO STDOUT WITH (FORMAT binary) — magic-prefixed output
