-- pg_forward_ref_manual_two_session.sql
--
-- Purpose:
--   Manual / multi-session probes that are hard to express in a single
--   self-contained SQL script. These are still written in harness-friendly SQL,
--   but they are intended to be split across Session A and Session B.
--
-- How to use:
--   1. Open two sessions connected to the same database.
--   2. Run the "Session A" blocks in one session and "Session B" blocks in
--      the other, following the comments in order.
--
-- These cases cover:
--   * transactional DDL visibility across sessions
--   * temp-table isolation across sessions
--   * optional security-definer / role-sensitive name resolution checks

DROP SCHEMA IF EXISTS test_forward_ref_manual CASCADE;
CREATE SCHEMA test_forward_ref_manual;

-------------------------------------------------------------------------------
-- M01 Session A: create function referencing future table, then create table
--                without commit yet.
-------------------------------------------------------------------------------
-- Session A:
SET search_path = test_forward_ref_manual, public;

BEGIN;

CREATE OR REPLACE FUNCTION m01_count_queue_servers()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM queue_servers;
  RETURN v_count;
END
$fn$;

CREATE TABLE queue_servers (
  id integer PRIMARY KEY
);

INSERT INTO queue_servers(id) VALUES (1), (2);

-- In Session A, before COMMIT:
-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT m01_count_queue_servers()::text AS row_count;

-------------------------------------------------------------------------------
-- M01 Session B: before Session A commits, function/table should not be visible
-------------------------------------------------------------------------------
-- Session B:
SET search_path = test_forward_ref_manual, public;

-- expect-error: 42883
SELECT m01_count_queue_servers();

-- expect-error: 42P01
SELECT count(*) FROM queue_servers;

-------------------------------------------------------------------------------
-- M01 Session A then Session B after commit
-------------------------------------------------------------------------------
-- Session A:
COMMIT;

-- Session B, after Session A committed:
-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT m01_count_queue_servers()::text AS row_count;

-------------------------------------------------------------------------------
-- M02 temp table isolation across sessions
-------------------------------------------------------------------------------
-- Session A:
CREATE OR REPLACE FUNCTION m02_count_temp()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM pg_temp.temp_items;
  RETURN v_count;
END
$fn$;

CREATE TEMP TABLE temp_items (id integer PRIMARY KEY);
INSERT INTO temp_items(id) VALUES (1), (2), (3);

-- begin-expected
-- columns: row_count
-- row: 3
-- end-expected
SELECT m02_count_temp()::text AS row_count;

-- Session B:
-- expect-error: 42P01
SELECT m02_count_temp();

-------------------------------------------------------------------------------
-- M03 optional security-definer name resolution probe
-------------------------------------------------------------------------------
-- This is optional because it requires privileges to create roles.
--
-- Session A (superuser or suitable admin role):
--   CREATE ROLE m03_owner LOGIN;
--   CREATE ROLE m03_caller LOGIN;
--   CREATE SCHEMA m03_owner_schema AUTHORIZATION m03_owner;
--   CREATE SCHEMA m03_shadow_schema AUTHORIZATION m03_caller;
--
--   SET ROLE m03_owner;
--   SET search_path = m03_owner_schema, public;
--   CREATE TABLE queue_servers(marker text primary key);
--   INSERT INTO queue_servers(marker) VALUES ('owner-table');
--
--   CREATE OR REPLACE FUNCTION m03_security_definer_lookup()
--   RETURNS text
--   LANGUAGE plpgsql
--   SECURITY DEFINER
--   AS $fn$
--   DECLARE v text;
--   BEGIN
--     SELECT marker INTO v FROM queue_servers LIMIT 1;
--     RETURN v;
--   END
--   $fn$;
--   RESET ROLE;
--
-- Session B:
--   SET ROLE m03_caller;
--   SET search_path = m03_shadow_schema, public;
--   CREATE TABLE queue_servers(marker text primary key);
--   INSERT INTO queue_servers(marker) VALUES ('caller-shadow');
--
--   -- Characterization question:
--   -- Does execution resolve against the definer's environment or caller's?
--   -- Upstream PostgreSQL behavior can depend on search_path management and
--   -- whether the function explicitly sets search_path.
--
--   SELECT m03_security_definer_lookup();
--
-- Recommendation:
--   Also test a hardened variant:
--     ALTER FUNCTION m03_security_definer_lookup() SET search_path = m03_owner_schema, pg_temp;
--   Then rerun and confirm deterministic lookup.
