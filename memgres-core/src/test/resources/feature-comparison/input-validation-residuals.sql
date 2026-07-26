-- ============================================================================
-- Feature Comparison: input PostgreSQL rejects
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A date cannot answer a sub-day unit, a tsvector position entry ends at a
-- comma or not at all, and a search_path naming no usable schema leaves a
-- CREATE with nowhere to go. Accepting these silently is worse than failing.
-- ============================================================================

-- ============================================================================
-- 1. extract() refuses a unit the type cannot answer
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: not supported for type date
-- end-expected-error
SELECT extract(hour FROM DATE '2026-06-25');

-- begin-expected
-- columns: a | b
-- row: 25, 2026
-- end-expected
SELECT extract(day FROM DATE '2026-06-25')::text AS a,
       extract(year FROM DATE '2026-06-25')::text AS b;

-- A timestamp does have a time of day
-- begin-expected
-- columns: a
-- row: 13
-- end-expected
SELECT extract(hour FROM TIMESTAMP '2026-06-25 13:00')::text AS a;

-- ============================================================================
-- 2. A tsvector position entry must be well formed
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector
-- end-expected-error
SELECT 'cat:1x'::tsvector;

-- begin-expected
-- columns: a | b
-- row: 'cat':1A, 'cat':1,2
-- end-expected
SELECT 'cat:1A'::tsvector::text AS a, 'cat:1,2'::tsvector::text AS b;

-- ============================================================================
-- 3. A CREATE needs a schema to land in
-- ============================================================================

SET search_path = nosuchschema;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: no schema has been selected to create in
-- end-expected-error
CREATE TABLE ivr_x (id int);

RESET search_path;

-- With a usable search_path the create succeeds
CREATE TABLE ivr_ok (id int);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'ivr_ok';

DROP TABLE ivr_ok;
