-- pg_forward_ref_signature_dependency.sql
--
-- Purpose:
--   Coverage for routine signatures that depend on object existence,
--   dependency tracking, SETOF/composite/domain/enum usage, defaults,
--   overloading, CREATE OR REPLACE, and DROP ... RESTRICT/CASCADE behavior.

DROP SCHEMA IF EXISTS test_forward_ref_sig CASCADE;
CREATE SCHEMA test_forward_ref_sig;
SET search_path = test_forward_ref_sig, public;

-------------------------------------------------------------------------------
-- S01: return type that does not exist must fail at CREATE time
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION s01_returns_future_type()
RETURNS s01_future_enum
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN NULL;
END
$fn$;

CREATE TYPE s01_future_enum AS ENUM ('one', 'two');

CREATE OR REPLACE FUNCTION s01_returns_future_type()
RETURNS s01_future_enum
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN 'one'::s01_future_enum;
END
$fn$;

-- begin-expected
-- columns: value
-- row: one
-- end-expected
SELECT s01_returns_future_type()::text AS value;

-------------------------------------------------------------------------------
-- S02: argument type that does not exist must fail at CREATE time
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION s02_accepts_future_type(p_val s02_future_domain)
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN p_val::text;
END
$fn$;

CREATE DOMAIN s02_future_domain AS text CHECK (VALUE <> '');

CREATE OR REPLACE FUNCTION s02_accepts_future_type(p_val s02_future_domain)
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN p_val::text;
END
$fn$;

-- begin-expected
-- columns: value
-- row: hello
-- end-expected
SELECT s02_accepts_future_type('hello'::s02_future_domain) AS value;

-------------------------------------------------------------------------------
-- S03: RETURNS SETOF future composite type fails until the relation exists
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION s03_setof_future_relation()
RETURNS SETOF s03_items
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN;
END
$fn$;

CREATE TABLE s03_items (
  id integer PRIMARY KEY,
  title text NOT NULL
);

INSERT INTO s03_items(id, title) VALUES (1, 'one'), (2, 'two');

CREATE OR REPLACE FUNCTION s03_setof_future_relation()
RETURNS SETOF s03_items
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN QUERY SELECT * FROM s03_items ORDER BY id;
END
$fn$;

-- begin-expected
-- columns: id,title
-- row: 1|one
-- row: 2|two
-- end-expected
SELECT id::text AS id, title
FROM s03_setof_future_relation();

-------------------------------------------------------------------------------
-- S04: OUT parameters using future type fail at CREATE time
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION s04_out_future_type(OUT val s04_future_enum)
LANGUAGE plpgsql
AS $fn$
BEGIN
  val := NULL;
END
$fn$;

CREATE TYPE s04_future_enum AS ENUM ('ready', 'done');

CREATE OR REPLACE FUNCTION s04_out_future_type(OUT val s04_future_enum)
LANGUAGE plpgsql
AS $fn$
BEGIN
  val := 'ready'::s04_future_enum;
END
$fn$;

-- begin-expected
-- columns: val
-- row: ready
-- end-expected
SELECT s04_out_future_type()::text AS val;

-------------------------------------------------------------------------------
-- S05: default argument expression referencing future function
-------------------------------------------------------------------------------
-- expect-error: 42883
CREATE OR REPLACE FUNCTION s05_with_default(p_text text DEFAULT s05_default_supplier())
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN p_text;
END
$fn$;

CREATE OR REPLACE FUNCTION s05_default_supplier()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN 'defaulted';
END
$fn$;

CREATE OR REPLACE FUNCTION s05_with_default(p_text text DEFAULT s05_default_supplier())
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN p_text;
END
$fn$;

-- begin-expected
-- columns: value
-- row: defaulted
-- end-expected
SELECT s05_with_default() AS value;

-------------------------------------------------------------------------------
-- S06: declaration %TYPE / %ROWTYPE create catalog dependencies
-------------------------------------------------------------------------------
CREATE TABLE s06_items (
  id integer PRIMARY KEY,
  title text NOT NULL
);

CREATE OR REPLACE FUNCTION s06_uses_rowtype()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  r s06_items%ROWTYPE;
BEGIN
  SELECT * INTO r FROM s06_items WHERE id = 1;
  RETURN r.title;
END
$fn$;

INSERT INTO s06_items(id, title) VALUES (1, 'dep-ok');

-- begin-expected
-- columns: value
-- row: dep-ok
-- end-expected
SELECT s06_uses_rowtype() AS value;

-- expect-error
DROP TABLE s06_items RESTRICT;

DROP TABLE s06_items CASCADE;

-------------------------------------------------------------------------------
-- S07: plain body-only reference may not block DROP ... RESTRICT the same way
-------------------------------------------------------------------------------
CREATE TABLE s07_items (
  id integer PRIMARY KEY,
  title text NOT NULL
);

CREATE OR REPLACE FUNCTION s07_body_reference_only()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT title INTO v FROM s07_items WHERE id = 1;
  RETURN v;
END
$fn$;

INSERT INTO s07_items(id, title) VALUES (1, 'runtime-lookup');

DROP TABLE s07_items RESTRICT;

-- expect-error: 42P01
SELECT s07_body_reference_only();

-------------------------------------------------------------------------------
-- S08: overloaded routine introduced later changes callable resolution
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION s08_pick(text)
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN 'text:' || $1;
END
$fn$;

-- begin-expected
-- columns: value
-- row: text:abc
-- end-expected
SELECT s08_pick('abc') AS value;

CREATE TYPE s08_payload AS (label text);

CREATE OR REPLACE FUNCTION s08_pick(s08_payload)
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN 'payload:' || ($1).label;
END
$fn$;

-- begin-expected
-- columns: value
-- row: payload:xyz
-- end-expected
SELECT s08_pick(ROW('xyz')::s08_payload) AS value;

-------------------------------------------------------------------------------
-- S09: CREATE OR REPLACE after schema evolution
-------------------------------------------------------------------------------
CREATE TABLE s09_items (
  id integer PRIMARY KEY,
  old_name text NOT NULL
);

INSERT INTO s09_items(id, old_name) VALUES (1, 'before');

CREATE OR REPLACE FUNCTION s09_read_name()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT old_name INTO v FROM s09_items WHERE id = 1;
  RETURN v;
END
$fn$;

-- begin-expected
-- columns: value
-- row: before
-- end-expected
SELECT s09_read_name() AS value;

ALTER TABLE s09_items RENAME COLUMN old_name TO new_name;

-- expect-error: 42703
SELECT s09_read_name();

CREATE OR REPLACE FUNCTION s09_read_name()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT new_name INTO v FROM s09_items WHERE id = 1;
  RETURN v;
END
$fn$;

-- begin-expected
-- columns: value
-- row: before
-- end-expected
SELECT s09_read_name() AS value;

-------------------------------------------------------------------------------
-- S10: polymorphic / record return with future relation reference in body
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION s10_record_future_ref()
RETURNS record
LANGUAGE plpgsql
AS $fn$
DECLARE
  r record;
BEGIN
  SELECT * INTO r FROM s10_items WHERE id = 1;
  RETURN r;
END
$fn$;

-- expect-error: 42P01
SELECT * FROM s10_record_future_ref() AS t(id integer, title text);

CREATE TABLE s10_items (
  id integer PRIMARY KEY,
  title text NOT NULL
);

INSERT INTO s10_items(id, title) VALUES (1, 'record-ok');

-- begin-expected
-- columns: id,title
-- row: 1|record-ok
-- end-expected
SELECT id::text AS id, title
FROM s10_record_future_ref() AS t(id integer, title text);
