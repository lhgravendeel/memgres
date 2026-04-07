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

-- DO blocks and dynamic SQL often used in migration/admin scripts
DO $$
BEGIN
  IF to_regclass('compat.do_t') IS NULL THEN
    EXECUTE 'CREATE TABLE compat.do_t(id int PRIMARY KEY, note text)';
  END IF;
END
$$;

DO $$
DECLARE
  tbl text := 'dyn_t';
BEGIN
  EXECUTE format('CREATE TABLE %I(id int, note text)', tbl);
  EXECUTE format('INSERT INTO %I VALUES ($1, $2)', tbl) USING 1, 'x';
END
$$;

SELECT * FROM do_t ORDER BY id;
SELECT * FROM dyn_t ORDER BY id;

DO $$
DECLARE
  col_exists boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'compat'
      AND table_name = 'do_t'
      AND column_name = 'created_at'
  ) INTO col_exists;

  IF NOT col_exists THEN
    EXECUTE 'ALTER TABLE compat.do_t ADD COLUMN created_at timestamptz DEFAULT CURRENT_TIMESTAMP';
  END IF;
END
$$;

SELECT * FROM do_t ORDER BY id;

DO $$
DECLARE
  t text := 'dyn_t';
  cnt int;
BEGIN
  EXECUTE format('SELECT count(*) FROM %I', t) INTO cnt;
  RAISE NOTICE 'count=%', cnt;
END
$$;

DO $$
BEGIN
  BEGIN
    EXECUTE 'INSERT INTO compat.do_t VALUES (1, ''dup'', CURRENT_TIMESTAMP)';
  EXCEPTION
    WHEN unique_violation THEN
      RAISE NOTICE 'duplicate ignored';
  END;
END
$$;

-- bad dynamic SQL / DO cases
DO $$ BEGIN EXECUTE 'CREATE TABLE compat.x('; END $$;
DO $$ BEGIN RAISE EXCEPTION 'boom'; END $$;
DO $$ BEGIN EXECUTE format('SELECT * FROM %I', NULL); END $$;
DO $$ BEGIN EXECUTE 'INSERT INTO compat.no_such VALUES (1)'; END $$;
DO $$ DECLARE x int BEGIN x := 1; END $$;
DO LANGUAGE plpgsql $$ BEGIN EXECUTE 'SELECT nope'; END $$;

DROP SCHEMA compat CASCADE;
