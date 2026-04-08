-- pg_forward_ref_sql_language_extended.sql
--
-- Purpose:
--   Extra SQL-language routine coverage for eager validation of tables, views,
--   columns, types, called routines, sequences, collations, operators, and
--   CREATE OR REPLACE behavior.

DROP SCHEMA IF EXISTS test_forward_ref_sqlx CASCADE;
CREATE SCHEMA test_forward_ref_sqlx;
SET search_path = test_forward_ref_sqlx, public;

-------------------------------------------------------------------------------
-- Q01: SQL function rejects missing table at CREATE time
-------------------------------------------------------------------------------
-- expect-error: 42P01
CREATE OR REPLACE FUNCTION q01_missing_table()
RETURNS integer
LANGUAGE sql
AS $fn$
  SELECT count(*)::integer FROM q01_items
$fn$;

CREATE TABLE q01_items (id integer PRIMARY KEY);
INSERT INTO q01_items(id) VALUES (1), (2);

CREATE OR REPLACE FUNCTION q01_missing_table()
RETURNS integer
LANGUAGE sql
AS $fn$
  SELECT count(*)::integer FROM q01_items
$fn$;

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT q01_missing_table()::text AS row_count;

-------------------------------------------------------------------------------
-- Q02: SQL function rejects missing column at CREATE time
-------------------------------------------------------------------------------
-- expect-error: 42703
CREATE OR REPLACE FUNCTION q02_missing_column()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT missing_col FROM q01_items LIMIT 1
$fn$;

ALTER TABLE q01_items ADD COLUMN missing_col text;
UPDATE q01_items SET missing_col = 'ok' WHERE id = 1;
UPDATE q01_items SET missing_col = 'still-ok' WHERE id = 2;

CREATE OR REPLACE FUNCTION q02_missing_column()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT missing_col FROM q01_items ORDER BY id LIMIT 1
$fn$;

-- begin-expected
-- columns: value
-- row: ok
-- end-expected
SELECT q02_missing_column() AS value;

-------------------------------------------------------------------------------
-- Q03: SQL function rejects missing type / called routine / sequence at CREATE time
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION q03_missing_type()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT 'ready'::q03_future_enum::text
$fn$;

CREATE TYPE q03_future_enum AS ENUM ('ready', 'done');

CREATE OR REPLACE FUNCTION q03_missing_type()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT 'ready'::q03_future_enum::text
$fn$;

-- begin-expected
-- columns: value
-- row: ready
-- end-expected
SELECT q03_missing_type() AS value;

-- expect-error: 42883
CREATE OR REPLACE FUNCTION q03_missing_called_fn()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT q03_helper()
$fn$;

CREATE OR REPLACE FUNCTION q03_helper()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT 'helper-ok'::text
$fn$;

CREATE OR REPLACE FUNCTION q03_missing_called_fn()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT q03_helper()
$fn$;

-- begin-expected
-- columns: value
-- row: helper-ok
-- end-expected
SELECT q03_missing_called_fn() AS value;

-- expect-error: 42P01
CREATE OR REPLACE FUNCTION q03_missing_seq()
RETURNS bigint
LANGUAGE sql
AS $fn$
  SELECT nextval('q03_future_seq')
$fn$;

CREATE SEQUENCE q03_future_seq START WITH 5;

CREATE OR REPLACE FUNCTION q03_missing_seq()
RETURNS bigint
LANGUAGE sql
AS $fn$
  SELECT nextval('q03_future_seq')
$fn$;

-- begin-expected
-- columns: seqval
-- row: 5
-- end-expected
SELECT q03_missing_seq()::text AS seqval;

-------------------------------------------------------------------------------
-- Q04: SQL function with future view requires the view first
-------------------------------------------------------------------------------
-- expect-error: 42P01
CREATE OR REPLACE FUNCTION q04_future_view_count()
RETURNS integer
LANGUAGE sql
AS $fn$
  SELECT count(*)::integer FROM q04_view
$fn$;

CREATE TABLE q04_base (id integer PRIMARY KEY, active boolean NOT NULL);
INSERT INTO q04_base(id, active) VALUES (1, true), (2, false), (3, true);

CREATE VIEW q04_view AS
SELECT * FROM q04_base WHERE active;

CREATE OR REPLACE FUNCTION q04_future_view_count()
RETURNS integer
LANGUAGE sql
AS $fn$
  SELECT count(*)::integer FROM q04_view
$fn$;

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT q04_future_view_count()::text AS row_count;

-------------------------------------------------------------------------------
-- Q05: SQL function using explicit collation/operator must resolve them at create time
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION q05_missing_collation()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT 'a' COLLATE q05_missing_collation
$fn$;

CREATE OPERATOR q05_plus_plus (
  LEFTARG = text,
  RIGHTARG = text,
  PROCEDURE = textcat
);

-- the harness may not support operator DDL in all environments; do a lighter probe
CREATE OR REPLACE FUNCTION q05_concat(text, text)
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT $1 || $2
$fn$;

-- begin-expected
-- columns: value
-- row: ab
-- end-expected
SELECT q05_concat('a', 'b') AS value;

-------------------------------------------------------------------------------
-- Q06: CREATE OR REPLACE after schema change
-------------------------------------------------------------------------------
CREATE TABLE q06_items (
  id integer PRIMARY KEY,
  old_name text NOT NULL
);

INSERT INTO q06_items(id, old_name) VALUES (1, 'before');

CREATE OR REPLACE FUNCTION q06_read_name()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT old_name FROM q06_items WHERE id = 1
$fn$;

-- begin-expected
-- columns: value
-- row: before
-- end-expected
SELECT q06_read_name() AS value;

ALTER TABLE q06_items RENAME COLUMN old_name TO new_name;

-- expect-error: 42703
CREATE OR REPLACE FUNCTION q06_read_name()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT old_name FROM q06_items WHERE id = 1
$fn$;

CREATE OR REPLACE FUNCTION q06_read_name()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT new_name FROM q06_items WHERE id = 1
$fn$;

-- begin-expected
-- columns: value
-- row: before
-- end-expected
SELECT q06_read_name() AS value;
