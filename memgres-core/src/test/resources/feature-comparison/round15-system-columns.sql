-- ============================================================================
-- Feature Comparison: Round 15 — System columns
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_sc CASCADE;
CREATE SCHEMA r15_sc;
SET search_path = r15_sc, public;

CREATE TABLE r15_sc_t (id int);
INSERT INTO r15_sc_t VALUES (1),(2),(3);

-- ============================================================================
-- SECTION A: ctid
-- ============================================================================

-- 1. ctid formatted as '(block,tuple)'
SELECT ctid::text AS v FROM r15_sc_t LIMIT 1;

-- 2. ctid type is 'tid'
-- begin-expected
-- columns: t
-- row: tid
-- end-expected
SELECT pg_typeof(ctid)::text AS t FROM r15_sc_t LIMIT 1;

-- 3. ctid unique per row
-- begin-expected
-- columns: d, t
-- row: 3, 3
-- end-expected
SELECT count(DISTINCT ctid)::int AS d, count(*)::int AS t FROM r15_sc_t;

-- ============================================================================
-- SECTION B: xmin / xmax / cmin / cmax
-- ============================================================================

-- 4. xmin readable and non-null
SELECT xmin FROM r15_sc_t LIMIT 1;

-- 5. xmax readable (may be 0)
SELECT xmax FROM r15_sc_t LIMIT 1;

-- 6. cmin / cmax readable
SELECT cmin, cmax FROM r15_sc_t LIMIT 1;

-- 7. xmin type is 'xid'
-- begin-expected
-- columns: t
-- row: xid
-- end-expected
SELECT pg_typeof(xmin)::text AS t FROM r15_sc_t LIMIT 1;

-- 8. cmin type is 'cid'
-- begin-expected
-- columns: t
-- row: cid
-- end-expected
SELECT pg_typeof(cmin)::text AS t FROM r15_sc_t LIMIT 1;

-- ============================================================================
-- SECTION C: tableoid
-- ============================================================================

-- 9. tableoid == pg_class.oid of relation
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (tableoid = 'r15_sc_t'::regclass::oid)::text AS ok FROM r15_sc_t LIMIT 1;

-- 10. tableoid type is 'oid'
-- begin-expected
-- columns: t
-- row: oid
-- end-expected
SELECT pg_typeof(tableoid)::text AS t FROM r15_sc_t LIMIT 1;

-- ============================================================================
-- SECTION D: Read-only (writes rejected)
-- ============================================================================

-- 11. Cannot UPDATE ctid
-- begin-expected-error
-- message-like: ctid
-- end-expected-error
UPDATE r15_sc_t SET ctid='(0,1)' WHERE id=1;

-- 12. Cannot INSERT into tableoid
-- begin-expected-error
-- message-like: tableoid
-- end-expected-error
INSERT INTO r15_sc_t (id, tableoid) VALUES (10, 0);

-- 13. Cannot UPDATE xmin
-- begin-expected-error
-- message-like: xmin
-- end-expected-error
UPDATE r15_sc_t SET xmin=0 WHERE id=1;
