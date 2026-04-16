-- ============================================================================
-- Feature Comparison: Round 16 — String / regex functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION K1: sha224
-- ============================================================================

-- 1. sha224('abc') hex known-answer
-- begin-expected
-- columns: v
-- row: 23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7
-- end-expected
SELECT encode(sha224('abc'::bytea), 'hex') AS v;

-- ============================================================================
-- SECTION K2: convert(bytea, src, dest) 3-arg form
-- ============================================================================

-- 2. UTF8 'é' → LATIN1 = 0xe9
-- begin-expected
-- columns: v
-- row: e9
-- end-expected
SELECT encode(convert('é'::bytea, 'UTF8', 'LATIN1'), 'hex') AS v;

-- ============================================================================
-- SECTION K3: regexp_instr
-- ============================================================================

-- 3. regexp_instr('banana','a',1,2) = 4
-- begin-expected
-- columns: p
-- row: 4
-- end-expected
SELECT regexp_instr('banana', 'a', 1, 2) AS p;

-- ============================================================================
-- SECTION K4: regexp_substr N-th occurrence
-- ============================================================================

-- 4. regexp_substr('foobarfoo','foo',1,2) = 'foo'
-- begin-expected
-- columns: v
-- row: foo
-- end-expected
SELECT regexp_substr('foobarfoo', 'foo', 1, 2) AS v;

-- ============================================================================
-- SECTION K5: regexp_count
-- ============================================================================

-- 5. regexp_count('banana','a') = 3
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT regexp_count('banana', 'a') AS n;

-- 6. regexp_count('banana','a',4) = 2
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT regexp_count('banana', 'a', 4) AS n;

-- ============================================================================
-- SECTION K6: regexp_replace with start + N
-- ============================================================================

-- 7. regexp_replace('banana','a','X',1,2) = 'banXna'
-- begin-expected
-- columns: v
-- row: banXna
-- end-expected
SELECT regexp_replace('banana', 'a', 'X', 1, 2) AS v;

-- ============================================================================
-- SECTION K7: regexp_split_to_table SRF
-- ============================================================================

-- 8. 4 rows from ('a,b,c,d', ',')
-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::int AS n FROM regexp_split_to_table('a,b,c,d', ',');

-- ============================================================================
-- SECTION K8: overlay(bytea PLACING bytea FROM int)
-- ============================================================================

-- 9. overlay preserves raw bytes (no string coercion)
-- begin-expected
-- columns: v
-- row: ffaabbffff
-- end-expected
SELECT encode(
    overlay('\xffffffffff'::bytea PLACING '\xaabb'::bytea FROM 2),
    'hex') AS v;
