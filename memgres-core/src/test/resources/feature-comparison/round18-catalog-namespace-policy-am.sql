-- ============================================================================
-- Feature Comparison: Round 18 — Namespace / policy / AM catalogs
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION T1: CREATE SCHEMA AUTHORIZATION → nspowner
-- ============================================================================

DROP SCHEMA IF EXISTS r18_nso CASCADE;
DROP ROLE IF EXISTS r18_nsrole;
CREATE ROLE r18_nsrole;
CREATE SCHEMA r18_nso AUTHORIZATION r18_nsrole;

-- 1. nspowner follows AUTHORIZATION
-- begin-expected
-- columns: rolname
-- row: r18_nsrole
-- end-expected
SELECT r.rolname FROM pg_namespace n
JOIN pg_roles r ON r.oid = n.nspowner
WHERE n.nspname='r18_nso';

-- ============================================================================
-- SECTION T2: GRANT ON SCHEMA → nspacl
-- ============================================================================

DROP SCHEMA IF EXISTS r18_nsa CASCADE;
DROP ROLE IF EXISTS r18_nsag;
CREATE ROLE r18_nsag;
CREATE SCHEMA r18_nsa;
GRANT USAGE ON SCHEMA r18_nsa TO r18_nsag;

-- 2. nspacl populated
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (array_to_string(nspacl, ',') LIKE '%r18_nsag%') AS ok
  FROM pg_namespace WHERE nspname='r18_nsa';

-- ============================================================================
-- SECTION T3: CREATE POLICY → pg_policy row
-- ============================================================================

DROP TABLE IF EXISTS r18_rlspol CASCADE;
CREATE TABLE r18_rlspol(a int, t text);
ALTER TABLE r18_rlspol ENABLE ROW LEVEL SECURITY;
CREATE POLICY r18_p1 ON r18_rlspol FOR SELECT USING (true);

-- 3. pg_policy has row for r18_p1
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_policy WHERE polname='r18_p1';

-- ============================================================================
-- SECTION T4: AS RESTRICTIVE → pg_policies.permissive='RESTRICTIVE'
-- ============================================================================

DROP TABLE IF EXISTS r18_rlsperm CASCADE;
CREATE TABLE r18_rlsperm(a int);
ALTER TABLE r18_rlsperm ENABLE ROW LEVEL SECURITY;
CREATE POLICY r18_pr ON r18_rlsperm AS RESTRICTIVE FOR SELECT USING (true);

-- 4. permissive = 'RESTRICTIVE'
-- begin-expected
-- columns: permissive
-- row: RESTRICTIVE
-- end-expected
SELECT permissive FROM pg_policies WHERE policyname='r18_pr';

-- ============================================================================
-- SECTION T5: pg_amop has btree entries
-- ============================================================================

-- 5. btree opclass contains many pg_amop rows
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) > 0) AS ok FROM pg_amop ao
JOIN pg_am am ON am.oid = ao.amopmethod WHERE am.amname='btree';

-- ============================================================================
-- SECTION T6: pg_amproc has btree entries
-- ============================================================================

-- 6. btree pg_amproc rows present
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) > 0) AS ok FROM pg_amproc ap
JOIN pg_am am ON am.oid = ap.amprocfamily WHERE am.amname='btree';

-- ============================================================================
-- SECTION T7: pg_opfamily has GIN entries
-- ============================================================================

-- 7. at least one GIN opfamily
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) > 0) AS ok FROM pg_opfamily f
JOIN pg_am am ON am.oid = f.opfmethod WHERE am.amname='gin';
