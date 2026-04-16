-- ============================================================================
-- Feature Comparison: Round 16 — Array / JSON / XML edge cases
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r16_ajx CASCADE;
CREATE SCHEMA r16_ajx;
SET search_path = r16_ajx, public;

-- ============================================================================
-- SECTION F1: Multi-dimensional array literal
-- ============================================================================

-- 1. array_dims of 2x2 literal
-- begin-expected
-- columns: d
-- row: [1:2][1:2]
-- end-expected
SELECT array_dims('{{1,2},{3,4}}'::int[][]) AS d;

-- 2. element (2,1) access
-- begin-expected
-- columns: v
-- row: 3
-- end-expected
SELECT ('{{1,2},{3,4}}'::int[][])[2][1] AS v;

-- 3. cardinality is total-element count
-- begin-expected
-- columns: c
-- row: 4
-- end-expected
SELECT cardinality('{{1,2},{3,4}}'::int[][]) AS c;

-- ============================================================================
-- SECTION F2: Array equality with NULL elements
-- ============================================================================

-- 4. ARRAY[1, NULL] = ARRAY[1, NULL] propagates NULL → result is NULL
-- begin-expected
-- columns: eq
-- row: NULL
-- end-expected
SELECT (ARRAY[1, NULL]::int[] = ARRAY[1, NULL]::int[]) AS eq;

-- 5. Non-null arrays with equal elements → TRUE
-- begin-expected
-- columns: eq
-- row: t
-- end-expected
SELECT (ARRAY[1,2,3] = ARRAY[1,2,3]) AS eq;

-- ============================================================================
-- SECTION F3: jsonb_populate_record / jsonb_populate_recordset
-- ============================================================================

CREATE TABLE r16_jp (a int, b text);

-- 6. jsonb_populate_record
-- begin-expected
-- columns: a, b
-- row: 42, hi
-- end-expected
SELECT a, b FROM jsonb_populate_record(NULL::r16_jp, '{"a":42,"b":"hi"}'::jsonb);

-- 7. jsonb_populate_recordset
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n
FROM jsonb_populate_recordset(NULL::r16_jp, '[{"a":1,"b":"x"},{"a":2,"b":"y"}]'::jsonb);

-- ============================================================================
-- SECTION F4: xmlserialize DOCUMENT + INDENT
-- ============================================================================

-- 8. DOCUMENT mode rejects multi-root
-- begin-expected-error
-- message-like: document
-- end-expected-error
SELECT xmlserialize(DOCUMENT '<a/><b/>'::xml AS text);

-- 9. INDENT option produces multi-line output
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (position(E'\n' IN xmlserialize(DOCUMENT '<a><b/></a>'::xml AS text INDENT)) > 0) AS ok;
