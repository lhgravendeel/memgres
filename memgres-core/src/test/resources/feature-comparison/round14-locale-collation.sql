-- ============================================================================
-- Feature Comparison: Round 14 — Locale / collation semantics
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_coll CASCADE;
CREATE SCHEMA r14_coll;
SET search_path = r14_coll, public;

-- ============================================================================
-- SECTION A: pg_collation columns
-- ============================================================================

-- 1. collversion column
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM information_schema.columns
  WHERE table_name = 'pg_collation' AND column_name = 'collversion';

-- 2. collprovider column
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM information_schema.columns
  WHERE table_name = 'pg_collation' AND column_name = 'collprovider';

-- 3. collisdeterministic column
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM information_schema.columns
  WHERE table_name = 'pg_collation' AND column_name = 'collisdeterministic';

-- ============================================================================
-- SECTION B: CREATE COLLATION
-- ============================================================================

-- 4. locale-based
CREATE COLLATION r14_coll_loc (locale = 'en_US.UTF-8');

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_collation WHERE collname = 'r14_coll_loc';

-- 5. provider = icu
CREATE COLLATION r14_coll_icu (provider = icu, locale = 'und-u-ks-level2');

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_collation WHERE collname = 'r14_coll_icu';

-- 6. deterministic = false
CREATE COLLATION r14_coll_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

-- begin-expected
-- columns: eq
-- row: t
-- end-expected
SELECT ('ABC' = 'abc' COLLATE r14_coll_ci)::text AS eq;

-- 7. FROM existing
CREATE COLLATION r14_coll_from FROM "C";

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_collation WHERE collname = 'r14_coll_from';

-- ============================================================================
-- SECTION C: Turkish / German casing
-- ============================================================================

-- 8. German capital sharp S → ß
-- begin-expected
-- columns: v
-- row: ẞ
-- end-expected
SELECT lower(U&'\1E9E') AS v;

-- 9. initcap
-- begin-expected
-- columns: v
-- row: Hello World
-- end-expected
SELECT initcap('hello world') AS v;

-- ============================================================================
-- SECTION D: normalize()
-- ============================================================================

-- 10. NFC of é is idempotent
-- begin-expected
-- columns: v
-- row: é
-- end-expected
SELECT normalize('é') AS v;

-- 11. NFKC: ligature ﬁ → fi
-- begin-expected
-- columns: v
-- row: fi
-- end-expected
SELECT normalize(U&'\FB01', NFKC) AS v;

-- 12. IS NFC NORMALIZED predicate
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('abc' IS NFC NORMALIZED)::text AS v;

-- ============================================================================
-- SECTION E: convert / convert_from / convert_to
-- ============================================================================

-- 13. convert_to UTF8
-- begin-expected
-- columns: v
-- row: \x616263
-- end-expected
SELECT convert_to('abc', 'UTF8')::text AS v;

-- 14. convert_from UTF8
-- begin-expected
-- columns: v
-- row: abc
-- end-expected
SELECT convert_from('\x616263'::bytea, 'UTF8') AS v;

-- ============================================================================
-- SECTION F: pg_client_encoding
-- ============================================================================

-- 15. client encoding
-- begin-expected
-- columns: v
-- row: UTF8
-- end-expected
SELECT pg_client_encoding() AS v;

-- ============================================================================
-- SECTION G: unicode_assigned (PG 17+)
-- ============================================================================

-- 16. 'A' assigned
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT unicode_assigned('A')::text AS v;

-- ============================================================================
-- SECTION H: unicode_version (PG 13+)
-- ============================================================================

-- 17. unicode_version returns non-null string
-- begin-expected
-- columns: has
-- row: t
-- end-expected
SELECT (unicode_version() IS NOT NULL)::text AS has;
