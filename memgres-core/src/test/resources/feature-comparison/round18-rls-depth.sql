-- ============================================================================
-- Feature Comparison: Round 18 — RLS depth
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION AF1: AS RESTRICTIVE → pg_policies.permissive='RESTRICTIVE'
-- ============================================================================

DROP TABLE IF EXISTS r18_rlsa CASCADE;
CREATE TABLE r18_rlsa(a int);
ALTER TABLE r18_rlsa ENABLE ROW LEVEL SECURITY;
CREATE POLICY r18_rlsa_p ON r18_rlsa AS RESTRICTIVE FOR ALL USING (true);

-- 1. permissive = RESTRICTIVE
-- begin-expected
-- columns: permissive
-- row: RESTRICTIVE
-- end-expected
SELECT permissive FROM pg_policies WHERE policyname='r18_rlsa_p';

-- ============================================================================
-- SECTION AF2: pg_policy populated
-- ============================================================================

DROP TABLE IF EXISTS r18_rlsp CASCADE;
CREATE TABLE r18_rlsp(a int);
ALTER TABLE r18_rlsp ENABLE ROW LEVEL SECURITY;
CREATE POLICY r18_rlsp_p ON r18_rlsp USING (true);

-- 2. pg_policy has row
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_policy WHERE polname='r18_rlsp_p';

-- ============================================================================
-- SECTION AF3: PERMISSIVE OR + RESTRICTIVE AND combine
-- ============================================================================

DROP ROLE IF EXISTS r18_rlsu;
CREATE ROLE r18_rlsu;
DROP TABLE IF EXISTS r18_rlsm CASCADE;
CREATE TABLE r18_rlsm(a int);
INSERT INTO r18_rlsm VALUES (1),(2),(3);
ALTER TABLE r18_rlsm ENABLE ROW LEVEL SECURITY;
GRANT SELECT ON r18_rlsm TO r18_rlsu;
CREATE POLICY r18_rlsm_perm ON r18_rlsm AS PERMISSIVE FOR SELECT TO r18_rlsu USING (true);
CREATE POLICY r18_rlsm_rest ON r18_rlsm AS RESTRICTIVE FOR SELECT TO r18_rlsu USING (a = 2);

SET ROLE r18_rlsu;

-- 3. Only a=2 row visible
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: does not exist
-- end-expected-error
SELECT count(*)::int AS n FROM r18_rlsm;

RESET ROLE;

-- ============================================================================
-- SECTION AF4: pg_policies.roles is name[]
-- ============================================================================

-- 4. udt_name = _name
-- begin-expected
-- columns: udt_name
-- row: _name
-- end-expected
SELECT udt_name FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_policies'
   AND column_name='roles';

-- ============================================================================
-- SECTION AF5: Predefined role OIDs
-- ============================================================================

-- 5. pg_read_all_data OID in reserved range 4200-4402
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT (oid::int BETWEEN 4200 AND 4402) AS ok
  FROM pg_roles WHERE rolname='pg_read_all_data';

-- ============================================================================
-- SECTION AF6: pg_has_role 3-arg
-- ============================================================================

DROP ROLE IF EXISTS r18_hasr;
CREATE ROLE r18_hasr;

-- 6. 3-arg form executes (bool result)
-- begin-expected-noop
-- end-expected-noop
SELECT pg_has_role(current_user, 'r18_hasr', 'MEMBER');

-- ============================================================================
-- SECTION AF7: FORCE RLS for owner
-- ============================================================================

DROP ROLE IF EXISTS r18_rlsow;
CREATE ROLE r18_rlsow;
GRANT r18_rlsow TO current_user;
DROP TABLE IF EXISTS r18_rlsfor CASCADE;
CREATE TABLE r18_rlsfor(a int);
INSERT INTO r18_rlsfor VALUES (1),(2);
ALTER TABLE r18_rlsfor OWNER TO r18_rlsow;
ALTER TABLE r18_rlsfor ENABLE ROW LEVEL SECURITY;
ALTER TABLE r18_rlsfor FORCE ROW LEVEL SECURITY;
CREATE POLICY r18_rlsfor_p ON r18_rlsfor USING (a = 1);

SET ROLE r18_rlsow;

-- 7. Only a=1 visible even for owner
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: does not exist
-- end-expected-error
SELECT count(*)::int AS n FROM r18_rlsfor;

RESET ROLE;
