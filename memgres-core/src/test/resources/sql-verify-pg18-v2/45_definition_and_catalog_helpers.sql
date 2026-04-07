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

CREATE TABLE def_help_t(
  id int PRIMARY KEY,
  note text DEFAULT 'x',
  qty int CHECK (qty >= 0)
);

CREATE VIEW def_help_v AS
SELECT id, note FROM def_help_t;

CREATE FUNCTION def_help_fn(a int) RETURNS int
LANGUAGE SQL
AS $$ SELECT a + 1 $$;

COMMENT ON TABLE def_help_t IS 'table comment';
COMMENT ON COLUMN def_help_t.note IS 'note comment';

CREATE INDEX def_help_note_idx ON def_help_t(note);

SELECT pg_get_viewdef('def_help_v'::regclass, true);
SELECT pg_get_indexdef('def_help_note_idx'::regclass);
SELECT pg_get_constraintdef((SELECT oid FROM pg_constraint WHERE conrelid = 'compat.def_help_t'::regclass AND contype = 'c'));
SELECT pg_get_functiondef('compat.def_help_fn(int)'::regprocedure);
SELECT obj_description('compat.def_help_t'::regclass);
SELECT col_description('compat.def_help_t'::regclass, 2);

SELECT adrelid::regclass, adnum, pg_get_expr(adbin, adrelid)
FROM pg_attrdef
WHERE adrelid = 'compat.def_help_t'::regclass
ORDER BY adnum;

SELECT c.relname, pg_get_viewdef(c.oid, true)
FROM pg_class c
WHERE c.oid = 'compat.def_help_v'::regclass;

-- bad helper cases
SELECT pg_get_viewdef('compat.missing_v'::regclass, true);
SELECT pg_get_indexdef('compat.missing_i'::regclass);
SELECT pg_get_functiondef('compat.missing_fn(int)'::regprocedure);
SELECT col_description('compat.def_help_t'::regclass, 99);

DROP SCHEMA compat CASCADE;
