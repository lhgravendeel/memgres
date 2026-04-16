-- ============================================================================
-- Feature Comparison: Round 13 — SQL Parser Gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- DDL / query syntax that PG 18 parses but Memgres rejects (or silently
-- misinterprets).
-- ============================================================================

DROP SCHEMA IF EXISTS r13_parser CASCADE;
CREATE SCHEMA r13_parser;
SET search_path = r13_parser, public;

-- ============================================================================
-- SECTION A: IS JSON predicate (SQL/JSON, PG 16+)
-- ============================================================================

-- 1. IS JSON basic true
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('{"a":1}' IS JSON)::text AS r;

-- 2. IS JSON basic false
-- begin-expected
-- columns: r
-- row: f
-- end-expected
SELECT ('not json' IS JSON)::text AS r;

-- 3. IS JSON OBJECT
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('{"a":1}' IS JSON OBJECT)::text AS r;

-- 4. IS JSON OBJECT false for array
-- begin-expected
-- columns: r
-- row: f
-- end-expected
SELECT ('[1,2,3]' IS JSON OBJECT)::text AS r;

-- 5. IS JSON ARRAY
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('[1,2,3]' IS JSON ARRAY)::text AS r;

-- 6. IS JSON SCALAR
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('42' IS JSON SCALAR)::text AS r;

-- 7. IS JSON NUMBER
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('42' IS JSON NUMBER)::text AS r;

-- 8. IS JSON STRING
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('"abc"' IS JSON STRING)::text AS r;

-- 9. IS JSON BOOLEAN
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('true' IS JSON BOOLEAN)::text AS r;

-- 10. IS JSON NULL
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('null' IS JSON NULL)::text AS r;

-- 11. IS NOT JSON negation
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('garbage' IS NOT JSON)::text AS r;

-- ============================================================================
-- SECTION B: Materialized CTE hints
-- ============================================================================

-- 12. WITH ... AS MATERIALIZED
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
WITH x AS MATERIALIZED (SELECT 1 AS a) SELECT a::text FROM x;

-- 13. WITH ... AS NOT MATERIALIZED
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
WITH x AS NOT MATERIALIZED (SELECT 1 AS a) SELECT a::text FROM x;

-- ============================================================================
-- SECTION C: CREATE STATISTICS
-- ============================================================================

CREATE TABLE r13_stats_base (a int, b int, c int);

-- 14. CREATE STATISTICS dependencies, ndistinct
-- begin-expected
-- columns: status
-- row: CREATE STATISTICS
-- end-expected
CREATE STATISTICS r13_stats_ab (dependencies, ndistinct) ON a, b FROM r13_stats_base;

-- 15. CREATE STATISTICS mcv kind
-- begin-expected
-- columns: status
-- row: CREATE STATISTICS
-- end-expected
CREATE STATISTICS r13_stats_mcv (mcv) ON a, b FROM r13_stats_base;

DROP STATISTICS IF EXISTS r13_stats_ab;
DROP STATISTICS IF EXISTS r13_stats_mcv;

-- ============================================================================
-- SECTION D: BEGIN ATOMIC function body
-- ============================================================================

-- 16. BEGIN ATOMIC single stmt
CREATE FUNCTION r13_atomic_fn() RETURNS int
  LANGUAGE SQL BEGIN ATOMIC SELECT 42; END;

-- begin-expected
-- columns: r
-- row: 42
-- end-expected
SELECT r13_atomic_fn()::text AS r;

DROP FUNCTION r13_atomic_fn();

-- 17. BEGIN ATOMIC without END → syntax error
-- begin-expected-error
-- message-like: syntax
-- end-expected-error
CREATE FUNCTION r13_bad_atomic() RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT 1;

-- ============================================================================
-- SECTION E: CREATE TYPE AS RANGE
-- ============================================================================

-- 18. Simple range type
CREATE TYPE r13_myrange AS RANGE (subtype = int4);

-- begin-expected
-- columns: r
-- row: [1,10)
-- end-expected
SELECT r13_myrange(1, 10)::text AS r;

DROP TYPE r13_myrange;

-- ============================================================================
-- SECTION F: Foreign data wrapper DDL
-- ============================================================================

-- 19. CREATE FOREIGN DATA WRAPPER
-- begin-expected
-- columns: status
-- row: CREATE FOREIGN DATA WRAPPER
-- end-expected
CREATE FOREIGN DATA WRAPPER r13_fdw;

-- 20. CREATE SERVER
-- begin-expected
-- columns: status
-- row: CREATE SERVER
-- end-expected
CREATE SERVER r13_srv FOREIGN DATA WRAPPER r13_fdw;

-- 21. CREATE FOREIGN TABLE
-- begin-expected
-- columns: status
-- row: CREATE FOREIGN TABLE
-- end-expected
CREATE FOREIGN TABLE r13_ftbl (id int, v text) SERVER r13_srv;

DROP FOREIGN TABLE r13_ftbl;
DROP SERVER r13_srv;
DROP FOREIGN DATA WRAPPER r13_fdw;

-- ============================================================================
-- SECTION G: Trigger transition tables
-- ============================================================================

CREATE TABLE r13_trig_tbl (id int);
CREATE FUNCTION r13_trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RETURN NULL; END $$;

-- 22. REFERENCING NEW TABLE AS ... FOR EACH STATEMENT
-- begin-expected
-- columns: status
-- row: CREATE TRIGGER
-- end-expected
CREATE TRIGGER r13_trig AFTER INSERT ON r13_trig_tbl
  REFERENCING NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION r13_trig_fn();

-- 23. REFERENCING OLD/NEW TABLE for UPDATE
-- begin-expected
-- columns: status
-- row: CREATE TRIGGER
-- end-expected
CREATE TRIGGER r13_trig2 AFTER UPDATE ON r13_trig_tbl
  REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION r13_trig_fn();
