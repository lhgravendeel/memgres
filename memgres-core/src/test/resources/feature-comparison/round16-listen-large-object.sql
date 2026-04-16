-- ============================================================================
-- Feature Comparison: Round 16 — Listen / Notify / Large objects
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION Q1: pg_listening_channels()
-- ============================================================================

LISTEN r16_ch;

-- 1. pg_listening_channels includes 'r16_ch'
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT EXISTS (
    SELECT 1 FROM pg_listening_channels() AS c WHERE c = 'r16_ch'
) AS ok;

UNLISTEN r16_ch;

-- ============================================================================
-- SECTION Q2: lo_truncate64
-- ============================================================================

-- 2. lo_truncate64 registered in pg_proc
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='lo_truncate64';

-- ============================================================================
-- SECTION Q3: lo_import / lo_export
-- ============================================================================

-- 3. lo_import registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='lo_import';

-- 4. lo_export registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='lo_export';

-- ============================================================================
-- SECTION Q4: GRANT ON LARGE OBJECT
-- ============================================================================

DROP ROLE IF EXISTS r16_lolu;
CREATE ROLE r16_lolu;
SELECT lo_create(987654);

-- 5. GRANT SELECT ON LARGE OBJECT parses
-- begin-expected-noop
-- end-expected-noop
GRANT SELECT ON LARGE OBJECT 987654 TO r16_lolu;

-- ============================================================================
-- SECTION Q5: pg_largeobject populated
-- ============================================================================

SELECT lo_put(987654, 0, 'hello'::bytea);

-- 6. pg_largeobject has at least one row for the LO
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1) AS ok FROM pg_largeobject WHERE loid = 987654;

SELECT lo_unlink(987654);
