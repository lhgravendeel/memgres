-- pg_forward_ref_positive_minimal.sql
--
-- Purpose:
--   Minimal positive regression test for forward-reference behavior:
--   plpgsql CREATE FUNCTION succeeds before the table exists, then executes
--   after the table is created.

DROP SCHEMA IF EXISTS test_forward_ref_pos CASCADE;
CREATE SCHEMA test_forward_ref_pos;
SET search_path = test_forward_ref_pos, public;

CREATE OR REPLACE FUNCTION get_first_server_uuid_plpgsql()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_uuid text;
BEGIN
  SELECT server_uuid
  INTO v_uuid
  FROM queue_servers
  ORDER BY server_uuid
  LIMIT 1;

  RETURN v_uuid;
END
$fn$;

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY,
  archived boolean NOT NULL DEFAULT false
);

INSERT INTO queue_servers(server_uuid, archived)
VALUES
  ('server-a', false),
  ('server-b', true),
  ('server-c', true);

-- begin-expected
-- columns: first_server_uuid
-- row: server-a
-- end-expected
SELECT get_first_server_uuid_plpgsql() AS first_server_uuid;

-- begin-expected
-- columns: archived_count
-- row: 2
-- end-expected
SELECT count(*)::text AS archived_count
FROM queue_servers
WHERE archived;
