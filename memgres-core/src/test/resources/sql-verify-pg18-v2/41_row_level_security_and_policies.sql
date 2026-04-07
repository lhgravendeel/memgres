\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE tenant_data(
  id int PRIMARY KEY,
  tenant_id int NOT NULL,
  note text,
  deleted_at timestamptz
);

INSERT INTO tenant_data VALUES
(1, 10, 'a', NULL),
(2, 20, 'b', NULL),
(3, 10, 'c', CURRENT_TIMESTAMP);

ALTER TABLE tenant_data ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_select_pol
ON tenant_data
FOR SELECT
USING (tenant_id = current_setting('compat.tenant_id')::int);

CREATE POLICY tenant_mod_pol
ON tenant_data
FOR UPDATE
USING (tenant_id = current_setting('compat.tenant_id')::int)
WITH CHECK (tenant_id = current_setting('compat.tenant_id')::int);

SET compat.tenant_id = '10';
SELECT * FROM tenant_data ORDER BY id;

ALTER TABLE tenant_data FORCE ROW LEVEL SECURITY;
SELECT * FROM tenant_data ORDER BY id;

ALTER POLICY tenant_select_pol
ON tenant_data
USING (tenant_id = current_setting('compat.tenant_id')::int AND deleted_at IS NULL);

SELECT * FROM tenant_data ORDER BY id;

DROP POLICY tenant_mod_pol ON tenant_data;
CREATE POLICY tenant_ins_pol
ON tenant_data
FOR INSERT
WITH CHECK (tenant_id = current_setting('compat.tenant_id')::int);

-- bad policy / RLS cases
CREATE POLICY bad_pol ON tenant_data USING (tenant_id = 1);
CREATE POLICY bad_cmd_pol
ON tenant_data
FOR UPSERT
USING (true);

ALTER POLICY no_such_pol ON tenant_data USING (true);
DROP POLICY no_such_pol ON tenant_data;
ALTER TABLE tenant_data DISABLE ROW SECURITY;
ALTER TABLE tenant_data NO FORCE ROW LEVEL SECURITY;

DROP SCHEMA compat CASCADE;
