-- ============================================================================
-- Feature Comparison: Round 18 — Role / auth catalog stubbing
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION S1: pg_authid columns
-- ============================================================================

-- 1. pg_authid exposes rolpassword column
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog'
   AND table_name='pg_authid'
   AND column_name='rolpassword';

-- 2. pg_authid exposes rolvaliduntil
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog'
   AND table_name='pg_authid'
   AND column_name='rolvaliduntil';

-- ============================================================================
-- SECTION S2: VALID UNTIL round-trips through pg_roles.rolvaliduntil
-- ============================================================================

DROP ROLE IF EXISTS r18_rv;
CREATE ROLE r18_rv VALID UNTIL '2099-01-01 00:00:00+00';

-- 3. rolvaliduntil reflects VALID UNTIL
-- begin-expected
-- columns: d
-- row: 2099-01-01
-- end-expected
SELECT to_char(rolvaliduntil, 'YYYY-MM-DD') AS d
  FROM pg_roles WHERE rolname='r18_rv';

-- ============================================================================
-- SECTION S3: CONNECTION LIMIT
-- ============================================================================

DROP ROLE IF EXISTS r18_rc;
CREATE ROLE r18_rc CONNECTION LIMIT 5;

-- 4. rolconnlimit reflects CONNECTION LIMIT 5
-- begin-expected
-- columns: rolconnlimit
-- row: 5
-- end-expected
SELECT rolconnlimit FROM pg_roles WHERE rolname='r18_rc';

-- ============================================================================
-- SECTION S4: rolconfig populated by ALTER ROLE SET
-- ============================================================================

DROP ROLE IF EXISTS r18_rf;
CREATE ROLE r18_rf;
ALTER ROLE r18_rf SET work_mem = '42MB';

-- 5. rolconfig array has work_mem=42MB entry
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (array_to_string(rolconfig, ',') LIKE '%work_mem=42MB%') AS ok
  FROM pg_roles WHERE rolname='r18_rf';

-- ============================================================================
-- SECTION S5: BYPASSRLS
-- ============================================================================

DROP ROLE IF EXISTS r18_rb;
CREATE ROLE r18_rb BYPASSRLS;

-- 6. rolbypassrls = true
-- begin-expected
-- columns: rolbypassrls
-- row: t
-- end-expected
SELECT rolbypassrls FROM pg_roles WHERE rolname='r18_rb';
