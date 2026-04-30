-- ============================================================================
-- Feature Comparison: Round 16 — Advanced DML / COPY fidelity
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r16_dml CASCADE;
CREATE SCHEMA r16_dml;
SET search_path = r16_dml, public;

-- ============================================================================
-- SECTION B1: ON CONFLICT DO UPDATE … WHERE predicate must be enforced
-- ============================================================================

CREATE TABLE r16_onc (id int PRIMARY KEY, v int, touched boolean DEFAULT false);
INSERT INTO r16_onc (id, v, touched) VALUES (1, 10, false);

-- 1. WHERE false → UPDATE must NOT run
INSERT INTO r16_onc (id, v) VALUES (1, 99)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, touched = true
WHERE false;

-- begin-expected
-- columns: v, touched
-- row: 10, f
-- end-expected
SELECT v, touched FROM r16_onc WHERE id = 1;

-- 2. WHERE true → UPDATE runs
CREATE TABLE r16_onc2 (id int PRIMARY KEY, v int);
INSERT INTO r16_onc2 VALUES (1, 10);
INSERT INTO r16_onc2 (id, v) VALUES (1, 99)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE true;

-- begin-expected
-- columns: v
-- row: 99
-- end-expected
SELECT v FROM r16_onc2 WHERE id = 1;

-- ============================================================================
-- SECTION B2: COPY ... WITH (ENCODING 'LATIN1') must actually honor encoding
-- ============================================================================

-- 3. 0xE9 byte in Latin-1 is 'é'; under UTF-8 it's an invalid sequence
--    (Shown here as a SQL-level statement; test is driven via CopyManager in Java)
CREATE TABLE r16_copyenc (s text);
-- COPY r16_copyenc FROM STDIN WITH (FORMAT text, ENCODING 'LATIN1');  -- [binary payload]

-- ============================================================================
-- SECTION B3: COPY ... HEADER MATCH must reject mismatched header
-- ============================================================================

-- 4. Header "wrong_name,b" must error when target columns are (a,b)
CREATE TABLE r16_hdrmatch (a int, b int);
-- COPY r16_hdrmatch (a, b) FROM STDIN WITH (FORMAT csv, HEADER MATCH);
-- wrong_name,b
-- 1,2
