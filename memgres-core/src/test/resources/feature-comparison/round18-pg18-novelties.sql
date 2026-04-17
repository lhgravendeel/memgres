-- ============================================================================
-- Feature Comparison: Round 18 — PG 18 novelty features
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION R1: uuidv4() / uuidv7() bare constructors
-- ============================================================================

-- 1. uuidv4() returns a v4 UUID
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (uuidv4()::text ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') AS ok;

-- 2. uuidv7() returns a v7 UUID (version nibble = 7)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (uuidv7()::text ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') AS ok;

-- ============================================================================
-- SECTION R2: uuid_extract_timestamp / uuid_extract_version
-- ============================================================================

-- 3. uuid_extract_version registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='uuid_extract_version';

-- 4. uuid_extract_timestamp registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='uuid_extract_timestamp';

-- ============================================================================
-- SECTION R3: ANY_VALUE aggregate
-- ============================================================================

DROP TABLE IF EXISTS r18_anyv;
CREATE TABLE r18_anyv(g int, v int);
INSERT INTO r18_anyv VALUES (1,10),(1,20),(2,30);

-- 5. any_value aggregate returns a member of the group
-- begin-expected
-- columns: g,ok
-- row: 1 | t
-- row: 2 | t
-- end-expected
SELECT g,
       (any_value(v) IN (10,20,30)) AS ok
  FROM r18_anyv
 GROUP BY g
 ORDER BY g;

-- ============================================================================
-- SECTION R4: has_largeobject_privilege
-- ============================================================================

-- 6. has_largeobject_privilege registered
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='has_largeobject_privilege';

-- ============================================================================
-- SECTION R5: log_lock_failures GUC
-- ============================================================================

-- 7. log_lock_failures present in pg_settings
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_settings WHERE name='log_lock_failures';

-- ============================================================================
-- SECTION R6: U&"..." unicode identifier and string
-- ============================================================================

-- 8. U&"d\0061t\0061" identifier == "data"
-- begin-expected
-- columns: data
-- row: 1
-- end-expected
SELECT 1 AS U&"d\0061t\0061";

-- 9. U&'d\0061t\0061' literal == 'data'
-- begin-expected
-- columns: s
-- row: data
-- end-expected
SELECT U&'d\0061t\0061' AS s;

-- ============================================================================
-- SECTION R7: Replication catalogs existence
-- ============================================================================

-- 10. pg_publication queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_publication;

-- 11. pg_subscription queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_subscription;
