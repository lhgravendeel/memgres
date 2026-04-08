-- pg_forward_ref_negative_create_only.sql
--
-- Purpose:
--   Minimal regression test for the negative case:
--   SQL-language CREATE FUNCTION should fail if the referenced table does not
--   yet exist.

DROP SCHEMA IF EXISTS test_forward_ref_neg CASCADE;
CREATE SCHEMA test_forward_ref_neg;
SET search_path = test_forward_ref_neg, public;

-- expect-error: 42P01
CREATE OR REPLACE FUNCTION get_first_server_uuid_sql()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT server_uuid
  FROM queue_servers
  ORDER BY server_uuid
  LIMIT 1
$fn$;
