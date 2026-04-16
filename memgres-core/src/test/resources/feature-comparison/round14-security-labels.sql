-- ============================================================================
-- Feature Comparison: Round 14 — SECURITY LABEL (unimplemented)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG supports SECURITY LABEL DDL and the pg_seclabel / pg_shseclabel catalogs.
-- Memgres has neither the DDL nor the catalogs.
-- ============================================================================

DROP SCHEMA IF EXISTS r14_seclab CASCADE;
CREATE SCHEMA r14_seclab;
SET search_path = r14_seclab, public;

-- ============================================================================
-- SECTION A: pg_seclabel / pg_shseclabel queryable
-- ============================================================================

-- 1. pg_seclabel exists and is empty
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_seclabel;

-- 2. pg_shseclabel exists and is empty
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_shseclabel;

-- 3. pg_seclabel has the 5 expected columns
-- begin-expected
-- columns: n
-- row: 5
-- end-expected
SELECT count(*)::text AS n FROM information_schema.columns
  WHERE table_schema = 'pg_catalog' AND table_name = 'pg_seclabel';

-- ============================================================================
-- SECTION B: SECURITY LABEL DDL parsing
-- ============================================================================

CREATE TABLE r14_sl_t (id int);

-- 4. SECURITY LABEL ON TABLE parses (default provider may error - ok)
-- begin-expected-error
-- message-like: provider
-- end-expected-error
SECURITY LABEL ON TABLE r14_sl_t IS 'secret';

CREATE TABLE r14_sl_c (id int, v text);

-- 5. SECURITY LABEL ON COLUMN parses
-- begin-expected-error
-- message-like: provider
-- end-expected-error
SECURITY LABEL ON COLUMN r14_sl_c.v IS 'confidential';

-- 6. SECURITY LABEL ON SCHEMA parses
-- begin-expected-error
-- message-like: provider
-- end-expected-error
SECURITY LABEL ON SCHEMA r14_seclab IS 'restricted';

CREATE OR REPLACE FUNCTION r14_sl_fn() RETURNS int AS 'SELECT 1' LANGUAGE SQL;

-- 7. SECURITY LABEL ON FUNCTION parses
-- begin-expected-error
-- message-like: provider
-- end-expected-error
SECURITY LABEL ON FUNCTION r14_sl_fn() IS 'sensitive';

-- 8. SECURITY LABEL ON ROLE parses (shared object → pg_shseclabel)
-- begin-expected-error
-- message-like: provider
-- end-expected-error
SECURITY LABEL ON ROLE memgres IS 'admin-role';

-- ============================================================================
-- SECTION C: SECURITY LABEL FOR <provider>
-- ============================================================================

CREATE TABLE r14_sl_prov (id int);

-- 9. SECURITY LABEL FOR selinux parses
-- begin-expected-error
-- message-like: provider
-- end-expected-error
SECURITY LABEL FOR selinux ON TABLE r14_sl_prov IS 'system_u:object_r:sepgsql_table_t:s0';

-- ============================================================================
-- SECTION D: SECURITY LABEL ... IS NULL clears
-- ============================================================================

CREATE TABLE r14_sl_nil (id int);

-- 10. Assignment then clear
-- begin-expected-error
-- message-like: provider
-- end-expected-error
SECURITY LABEL ON TABLE r14_sl_nil IS NULL;
