-- ============================================================================
-- Feature Comparison: Round 14 — Row-level security advanced
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_rls CASCADE;
CREATE SCHEMA r14_rls;
SET search_path = r14_rls, public;

-- ============================================================================
-- SECTION A: PERMISSIVE vs RESTRICTIVE combining
-- ============================================================================

CREATE ROLE r14_rls_u1;
CREATE TABLE r14_rls_p (id int, tag text);
INSERT INTO r14_rls_p VALUES (1,'a'),(2,'b'),(3,'c');
ALTER TABLE r14_rls_p ENABLE ROW LEVEL SECURITY;
GRANT SELECT ON r14_rls_p TO r14_rls_u1;
CREATE POLICY r14_rls_p1 ON r14_rls_p FOR SELECT TO r14_rls_u1 USING (tag = 'a');
CREATE POLICY r14_rls_p2 ON r14_rls_p FOR SELECT TO r14_rls_u1 USING (tag = 'b');

-- 1. Two PERMISSIVE policies OR-combine — pg_policies shows both rows
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::text AS c FROM pg_policies WHERE tablename = 'r14_rls_p';

CREATE ROLE r14_rls_u2;
CREATE TABLE r14_rls_r (id int, tag text, secret boolean);
INSERT INTO r14_rls_r VALUES (1,'a',false),(2,'a',true),(3,'b',false);
ALTER TABLE r14_rls_r ENABLE ROW LEVEL SECURITY;
GRANT SELECT ON r14_rls_r TO r14_rls_u2;
CREATE POLICY r14_rls_rp1 ON r14_rls_r AS PERMISSIVE FOR SELECT TO r14_rls_u2 USING (tag = 'a');
CREATE POLICY r14_rls_rp2 ON r14_rls_r AS RESTRICTIVE FOR SELECT TO r14_rls_u2 USING (NOT secret);

-- 2. Both PERMISSIVE and RESTRICTIVE present
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::text AS c FROM pg_policies WHERE tablename = 'r14_rls_r';

-- ============================================================================
-- SECTION B: FORCE ROW LEVEL SECURITY
-- ============================================================================

CREATE TABLE r14_rls_f (id int, owner_name text);
INSERT INTO r14_rls_f VALUES (1,'alice'),(2,'bob');
ALTER TABLE r14_rls_f ENABLE ROW LEVEL SECURITY;
ALTER TABLE r14_rls_f FORCE ROW LEVEL SECURITY;
CREATE POLICY r14_rls_fp ON r14_rls_f FOR SELECT USING (owner_name = current_user);

-- 3. pg_class.relforcerowsecurity reflects FORCE
-- begin-expected
-- columns: f
-- row: t
-- end-expected
SELECT relforcerowsecurity::text AS f FROM pg_class WHERE relname = 'r14_rls_f';

CREATE TABLE r14_rls_nf (id int);
ALTER TABLE r14_rls_nf ENABLE ROW LEVEL SECURITY;
ALTER TABLE r14_rls_nf FORCE ROW LEVEL SECURITY;
ALTER TABLE r14_rls_nf NO FORCE ROW LEVEL SECURITY;

-- 4. NO FORCE reverts flag
-- begin-expected
-- columns: f
-- row: f
-- end-expected
SELECT relforcerowsecurity::text AS f FROM pg_class WHERE relname = 'r14_rls_nf';

-- ============================================================================
-- SECTION C: BYPASSRLS role attribute
-- ============================================================================

CREATE ROLE r14_rls_bp WITH BYPASSRLS;

-- 5. BYPASSRLS at CREATE
-- begin-expected
-- columns: b
-- row: t
-- end-expected
SELECT rolbypassrls::text AS b FROM pg_roles WHERE rolname = 'r14_rls_bp';

CREATE ROLE r14_rls_bp2;
ALTER ROLE r14_rls_bp2 WITH BYPASSRLS;

-- 6. BYPASSRLS via ALTER
-- begin-expected
-- columns: b
-- row: t
-- end-expected
SELECT rolbypassrls::text AS b FROM pg_roles WHERE rolname = 'r14_rls_bp2';

-- ============================================================================
-- SECTION D: ALTER POLICY mutability
-- ============================================================================

CREATE TABLE r14_rls_ap (id int);
ALTER TABLE r14_rls_ap ENABLE ROW LEVEL SECURITY;
CREATE POLICY r14_rls_app ON r14_rls_ap USING (id > 0);
ALTER POLICY r14_rls_app ON r14_rls_ap USING (id > 10);

-- 7. Policy survives USING change
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_policies
  WHERE tablename = 'r14_rls_ap' AND policyname = 'r14_rls_app';

CREATE TABLE r14_rls_ren (id int);
ALTER TABLE r14_rls_ren ENABLE ROW LEVEL SECURITY;
CREATE POLICY r14_rls_ren_old ON r14_rls_ren USING (true);
ALTER POLICY r14_rls_ren_old ON r14_rls_ren RENAME TO r14_rls_ren_new;

-- 8. ALTER POLICY RENAME TO
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_policies
  WHERE tablename = 'r14_rls_ren' AND policyname = 'r14_rls_ren_new';

CREATE TABLE r14_rls_cc (id int);
ALTER TABLE r14_rls_cc ENABLE ROW LEVEL SECURITY;
CREATE POLICY r14_rls_ccp ON r14_rls_cc FOR SELECT USING (true);

-- 9. ALTER POLICY cannot change command action
-- begin-expected-error
-- message-like: syntax
-- end-expected-error
ALTER POLICY r14_rls_ccp ON r14_rls_cc RENAME TO r14_rls_ccp2 FOR UPDATE;

-- ============================================================================
-- SECTION E: view security_invoker (PG 15+)
-- ============================================================================

CREATE TABLE r14_rls_v (id int);
CREATE VIEW r14_rls_v_view WITH (security_invoker = true) AS SELECT * FROM r14_rls_v;

-- 10. security_invoker recorded in reloptions
-- begin-expected
-- columns: has_opt
-- row: t
-- end-expected
SELECT (reloptions::text LIKE '%security_invoker%')::text AS has_opt
  FROM pg_class WHERE relname = 'r14_rls_v_view';

-- ============================================================================
-- SECTION F: information_schema role views
-- ============================================================================

-- 11. applicable_roles queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0)::text AS ok FROM information_schema.applicable_roles;

-- 12. role_table_grants queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0)::text AS ok FROM information_schema.role_table_grants;
