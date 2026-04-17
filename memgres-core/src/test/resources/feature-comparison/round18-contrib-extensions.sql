-- ============================================================================
-- Feature Comparison: Round 18 — Contrib extensions surface
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION X1: fuzzystrmatch
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- 1. levenshtein works
-- begin-expected
-- columns: d
-- row: 3
-- end-expected
SELECT levenshtein('kitten','sitting') AS d;

-- 2. soundex works
-- begin-expected
-- columns: s
-- row: R163
-- end-expected
SELECT soundex('Robert') AS s;

-- ============================================================================
-- SECTION X2: unaccent
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS unaccent;

-- 3. unaccent strips diacritics
-- begin-expected
-- columns: s
-- row: cafe
-- end-expected
SELECT unaccent('café') AS s;

-- ============================================================================
-- SECTION X3: intarray
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS intarray;

-- 4. int[] & int[] intersects
-- begin-expected
-- columns: r
-- row: {2,3}
-- end-expected
SELECT (ARRAY[1,2,3]::int[] & ARRAY[2,3,4]::int[])::text AS r;

-- ============================================================================
-- SECTION X4: btree_gin
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS btree_gin;

-- 5. btree_gin registers gin integer_ops
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_opfamily f
JOIN pg_am a ON a.oid=f.opfmethod
WHERE a.amname='gin' AND f.opfname='integer_ops';

-- ============================================================================
-- SECTION X5: btree_gist
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 6. btree_gist registers gist integer_ops
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_opfamily f
JOIN pg_am a ON a.oid=f.opfmethod
WHERE a.amname='gist' AND f.opfname='integer_ops';

-- ============================================================================
-- SECTION X6: tablefunc.crosstab
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS tablefunc;

-- 7. crosstab registered
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='crosstab';

-- ============================================================================
-- SECTION X7: pg_stat_statements
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- 8. view queryable
-- begin-expected-error
-- message-like: must be
-- end-expected-error
SELECT (count(*) >= 0) AS ok FROM pg_stat_statements;

-- ============================================================================
-- SECTION X8: pg_buffercache
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_buffercache;

-- 9. view queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_buffercache;

-- ============================================================================
-- SECTION X9: pgrowlocks
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgrowlocks;

-- 10. pgrowlocks registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pgrowlocks';
