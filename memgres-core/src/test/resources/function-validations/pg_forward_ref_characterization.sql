-- pg_forward_ref_characterization.sql
--
-- Purpose:
--   Characterize what is validated at CREATE FUNCTION time versus first
--   execution time for PL/pgSQL, with emphasis on row types, search_path,
--   views, temp tables, and function-to-function dependencies.

DROP SCHEMA IF EXISTS test_forward_ref_diag CASCADE;
CREATE SCHEMA test_forward_ref_diag;
SET search_path = test_forward_ref_diag, public;

-------------------------------------------------------------------------------
-- C01: static SQL referencing missing table is accepted at CREATE time
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c01_missing_table_static_sql()
RETURNS integer
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count integer;
BEGIN
  SELECT count(*) INTO v_count FROM c01_future_table;
  RETURN v_count;
END
$fn$;

-- begin-expected
-- columns: routine_name
-- row: c01_missing_table_static_sql
-- end-expected
SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = 'test_forward_ref_diag'
  AND routine_name = 'c01_missing_table_static_sql';

-- expect-error: 42P01
SELECT c01_missing_table_static_sql();

CREATE TABLE c01_future_table (id integer PRIMARY KEY);
INSERT INTO c01_future_table(id) VALUES (1), (2), (3);

-- begin-expected
-- columns: row_count
-- row: 3
-- end-expected
SELECT c01_missing_table_static_sql()::text AS row_count;

-------------------------------------------------------------------------------
-- C02: static SQL referencing missing column is accepted at CREATE time
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c02_missing_column_static_sql()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_value text;
BEGIN
  SELECT t.missing_col INTO v_value
  FROM c02_table t
  WHERE t.id = 1;

  RETURN v_value;
END
$fn$;

CREATE TABLE c02_table (id integer PRIMARY KEY);
INSERT INTO c02_table(id) VALUES (1);

-- expect-error: 42703
SELECT c02_missing_column_static_sql();

ALTER TABLE c02_table ADD COLUMN missing_col text;
UPDATE c02_table SET missing_col = 'ok' WHERE id = 1;

-- begin-expected
-- columns: value
-- row: ok
-- end-expected
SELECT c02_missing_column_static_sql() AS value;

-------------------------------------------------------------------------------
-- C03: static SQL referencing future type in expression is accepted at CREATE time
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c03_future_type_in_query()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_value text;
BEGIN
  SELECT ('ready'::c03_future_enum)::text INTO v_value;
  RETURN v_value;
END
$fn$;

-- expect-error: 42704
SELECT c03_future_type_in_query();

CREATE TYPE c03_future_enum AS ENUM ('ready', 'paused');

-- begin-expected
-- columns: value
-- row: ready
-- end-expected
SELECT c03_future_type_in_query() AS value;

-------------------------------------------------------------------------------
-- C04: declaration-level references are validated immediately
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION c04_missing_declared_type()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_missing c04_type;
BEGIN
  RETURN 'x';
END
$fn$;

-- expect-error: 42704
CREATE OR REPLACE FUNCTION c04_missing_return_type()
RETURNS c04_type
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN NULL;
END
$fn$;

-------------------------------------------------------------------------------
-- C05: dynamic SQL defers parse/analysis to runtime
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c05_dynamic_missing_table()
RETURNS integer
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count integer;
BEGIN
  EXECUTE 'SELECT count(*) FROM c05_future_table' INTO v_count;
  RETURN v_count;
END
$fn$;

CREATE OR REPLACE FUNCTION c05_dynamic_bad_syntax()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
  EXECUTE 'SELECT FROM';
END
$fn$;

-- expect-error: 42P01
SELECT c05_dynamic_missing_table();

CREATE TABLE c05_future_table (id integer PRIMARY KEY);
INSERT INTO c05_future_table(id) VALUES (10), (11);

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT c05_dynamic_missing_table()::text AS row_count;

-- expect-error: 42601
SELECT c05_dynamic_bad_syntax();

-------------------------------------------------------------------------------
-- C06: SQL-language functions validate query objects immediately
-------------------------------------------------------------------------------
-- expect-error: 42P01
CREATE OR REPLACE FUNCTION c06_sql_missing_table()
RETURNS integer
LANGUAGE sql
AS $fn$
  SELECT count(*) FROM c06_no_table
$fn$;

CREATE TABLE c06_table (id integer PRIMARY KEY, value text);
INSERT INTO c06_table(id, value) VALUES (1, 'a');

-- expect-error: 42703
CREATE OR REPLACE FUNCTION c06_sql_missing_column()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT missing_col FROM c06_table WHERE id = 1
$fn$;

-------------------------------------------------------------------------------
-- C07: %TYPE and %ROWTYPE require relation/column resolution up front
-------------------------------------------------------------------------------
-- expect-error: 42P01
CREATE OR REPLACE FUNCTION c07_missing_percent_rowtype()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  r c07_future_table%ROWTYPE;
BEGIN
  RETURN 'x';
END
$fn$;

CREATE TABLE c07_future_table (
  id integer PRIMARY KEY,
  payload text NOT NULL
);

-- expect-error: 42703
CREATE OR REPLACE FUNCTION c07_missing_percent_type_column()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v c07_future_table.nope%TYPE;
BEGIN
  RETURN v;
END
$fn$;

CREATE OR REPLACE FUNCTION c07_existing_percent_rowtype()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  r c07_future_table%ROWTYPE;
BEGIN
  SELECT * INTO r FROM c07_future_table WHERE id = 1;
  RETURN r.payload;
END
$fn$;

INSERT INTO c07_future_table(id, payload) VALUES (1, 'rowtype-ok');

-- begin-expected
-- columns: payload
-- row: rowtype-ok
-- end-expected
SELECT c07_existing_percent_rowtype() AS payload;

-------------------------------------------------------------------------------
-- C08: RETURNS composite table type requires the type to exist at create time
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION c08_returns_future_table_type()
RETURNS c08_future_table
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN NULL;
END
$fn$;

CREATE TABLE c08_future_table (
  id integer PRIMARY KEY,
  payload text NOT NULL
);

CREATE OR REPLACE FUNCTION c08_returns_existing_table_type()
RETURNS c08_future_table
LANGUAGE plpgsql
AS $fn$
DECLARE
  r c08_future_table%ROWTYPE;
BEGIN
  SELECT * INTO r FROM c08_future_table WHERE id = 1;
  RETURN r;
END
$fn$;

INSERT INTO c08_future_table(id, payload) VALUES (1, 'composite-ok');

-- begin-expected
-- columns: id,payload
-- row: 1|composite-ok
-- end-expected
SELECT (c08_returns_existing_table_type()).id::text AS id,
       (c08_returns_existing_table_type()).payload AS payload;

-------------------------------------------------------------------------------
-- C09: function calling future function should fail when called, then succeed later
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c09_outer_calls_inner()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN c09_inner();
END
$fn$;

-- expect-error: 42883
SELECT c09_outer_calls_inner();

CREATE OR REPLACE FUNCTION c09_inner()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN 'inner-ok';
END
$fn$;

-- begin-expected
-- columns: value
-- row: inner-ok
-- end-expected
SELECT c09_outer_calls_inner() AS value;

-------------------------------------------------------------------------------
-- C10: view can be referenced before creation; execution defers until view exists
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c10_count_future_view()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM c10_future_view;
  RETURN v_count;
END
$fn$;

-- expect-error: 42P01
SELECT c10_count_future_view();

CREATE TABLE c10_base (id integer PRIMARY KEY);
INSERT INTO c10_base(id) VALUES (1), (2), (3), (4);
CREATE VIEW c10_future_view AS SELECT * FROM c10_base WHERE id <= 3;

-- begin-expected
-- columns: row_count
-- row: 3
-- end-expected
SELECT c10_count_future_view()::text AS row_count;

-------------------------------------------------------------------------------
-- C11: temp tables resolve at execution time within the current session
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c11_temp_count()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM pg_temp.c11_temp_table;
  RETURN v_count;
END
$fn$;

-- expect-error: 42P01
SELECT c11_temp_count();

CREATE TEMP TABLE c11_temp_table (id integer PRIMARY KEY);
INSERT INTO c11_temp_table(id) VALUES (1), (2);

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT c11_temp_count()::text AS row_count;

DROP TABLE c11_temp_table;

-- expect-error: 42P01
SELECT c11_temp_count();

-------------------------------------------------------------------------------
-- C12: search_path-sensitive name resolution
-------------------------------------------------------------------------------
CREATE SCHEMA c12_a;
CREATE SCHEMA c12_b;

CREATE TABLE c12_a.queue_servers (marker text PRIMARY KEY);
CREATE TABLE c12_b.queue_servers (marker text PRIMARY KEY);

INSERT INTO c12_a.queue_servers(marker) VALUES ('from-a');
INSERT INTO c12_b.queue_servers(marker) VALUES ('from-b');

CREATE OR REPLACE FUNCTION c12_unqualified_marker()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_marker text;
BEGIN
  SELECT marker INTO v_marker
  FROM queue_servers
  LIMIT 1;
  RETURN v_marker;
END
$fn$;

SET search_path = c12_a, test_forward_ref_diag, public;

-- begin-expected
-- columns: marker
-- row: from-a
-- end-expected
SELECT c12_unqualified_marker() AS marker;

SET search_path = c12_b, test_forward_ref_diag, public;

-- begin-expected
-- columns: marker
-- row: from-b
-- end-expected
SELECT c12_unqualified_marker() AS marker;

SET search_path = test_forward_ref_diag, public;

-------------------------------------------------------------------------------
-- C13: set-returning function with body referencing future table
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION c13_srf_future_table()
RETURNS TABLE(id integer, payload text)
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN QUERY
  SELECT t.id, t.payload
  FROM c13_future_table t
  ORDER BY t.id;
END
$fn$;

-- expect-error: 42P01
SELECT * FROM c13_srf_future_table();

CREATE TABLE c13_future_table (
  id integer PRIMARY KEY,
  payload text NOT NULL
);

INSERT INTO c13_future_table(id, payload)
VALUES (1, 'one'), (2, 'two');

-- begin-expected
-- columns: id,payload
-- row: 1|one
-- row: 2|two
-- end-expected
SELECT id::text AS id, payload
FROM c13_srf_future_table();

-------------------------------------------------------------------------------
-- C14: domain and enum references in declarations are immediate; in query text can defer
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION c14_declared_domain_var()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v c14_future_domain;
BEGIN
  RETURN NULL;
END
$fn$;

CREATE OR REPLACE FUNCTION c14_cast_to_future_domain()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT ('abc'::c14_future_domain)::text INTO v;
  RETURN v;
END
$fn$;

-- expect-error: 42704
SELECT c14_cast_to_future_domain();

CREATE DOMAIN c14_future_domain AS text CHECK (VALUE <> '');

-- begin-expected
-- columns: value
-- row: abc
-- end-expected
SELECT c14_cast_to_future_domain() AS value;
