-- ============================================================================
-- Feature Comparison: privileges are per-table, and views use owner's rights
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A grant names one schema's table, so it must never open a same-named table in
-- another schema. And PostgreSQL reads a view with the view owner's rights, so a
-- grant on the view alone is enough — the reader needs nothing on the base.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS pss1 CASCADE;
DROP SCHEMA IF EXISTS pss2 CASCADE;
DROP TABLE IF EXISTS pss_base CASCADE;

CREATE SCHEMA pss1;
CREATE SCHEMA pss2;
CREATE TABLE pss1.shared (id int);
CREATE TABLE pss2.shared (id int);
INSERT INTO pss1.shared VALUES (1);
INSERT INTO pss2.shared VALUES (2);

CREATE ROLE pss_alice LOGIN;
GRANT USAGE ON SCHEMA pss1 TO pss_alice;
GRANT USAGE ON SCHEMA pss2 TO pss_alice;

-- ============================================================================
-- 1. A grant on one schema's table does not reach the same name elsewhere
-- ============================================================================

GRANT SELECT ON pss1.shared TO pss_alice;

-- begin-expected
-- columns: on_ps1 | on_ps2
-- row: t, f
-- end-expected
SELECT has_table_privilege('pss_alice','pss1.shared','SELECT') AS on_ps1,
       has_table_privilege('pss_alice','pss2.shared','SELECT') AS on_ps2;

-- Granting the other one too, then revoking the first, leaves them independent
GRANT SELECT ON pss2.shared TO pss_alice;
REVOKE SELECT ON pss1.shared FROM pss_alice;

-- begin-expected
-- columns: on_ps1 | on_ps2
-- row: f, t
-- end-expected
SELECT has_table_privilege('pss_alice','pss1.shared','SELECT') AS on_ps1,
       has_table_privilege('pss_alice','pss2.shared','SELECT') AS on_ps2;

-- ============================================================================
-- 2. A view is read with its owner's rights
-- ============================================================================

CREATE TABLE pss_base (id int);
INSERT INTO pss_base VALUES (7);
CREATE VIEW pss_view AS SELECT * FROM pss_base;
GRANT SELECT ON pss_view TO pss_alice;

-- begin-expected
-- columns: on_view | on_base
-- row: t, f
-- end-expected
SELECT has_table_privilege('pss_alice','pss_view','SELECT') AS on_view,
       has_table_privilege('pss_alice','pss_base','SELECT') AS on_base;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP VIEW pss_view;
DROP TABLE pss_base CASCADE;
REVOKE ALL ON pss2.shared FROM pss_alice;
REVOKE USAGE ON SCHEMA pss1 FROM pss_alice;
REVOKE USAGE ON SCHEMA pss2 FROM pss_alice;
DROP SCHEMA pss1 CASCADE;
DROP SCHEMA pss2 CASCADE;
DROP ROLE pss_alice;
