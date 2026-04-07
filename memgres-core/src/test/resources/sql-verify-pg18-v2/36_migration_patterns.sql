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

-- idempotent DDL and patch-style schema evolution
CREATE TABLE IF NOT EXISTS patch_t(
  id int PRIMARY KEY,
  note text
);

CREATE TABLE IF NOT EXISTS patch_t(
  id int PRIMARY KEY,
  note text
);

ALTER TABLE patch_t
  ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE patch_t
  ADD COLUMN IF NOT EXISTS status text DEFAULT 'new';

CREATE INDEX IF NOT EXISTS patch_t_note_idx ON patch_t(note);
CREATE INDEX IF NOT EXISTS patch_t_note_idx ON patch_t(note);

DROP INDEX IF EXISTS missing_idx;
DROP TABLE IF EXISTS missing_table;

INSERT INTO patch_t(id, note) VALUES (1, 'a'), (2, 'b');
UPDATE patch_t SET status = 'backfilled' WHERE status = 'new';

ALTER TABLE patch_t
  ALTER COLUMN status SET NOT NULL;

ALTER TABLE patch_t
  ADD CONSTRAINT patch_t_note_uq UNIQUE (note);

SELECT to_regclass('compat.patch_t');
SELECT to_regclass('compat.patch_t_note_idx');

SELECT column_name, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'compat' AND table_name = 'patch_t'
ORDER BY ordinal_position;

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'compat.patch_t'::regclass
ORDER BY conname;

-- create or replace patterns often used in repeatable patches
CREATE OR REPLACE VIEW patch_v AS
SELECT id, note, status FROM patch_t;

CREATE OR REPLACE VIEW patch_v AS
SELECT id, note, status, created_at FROM patch_t;

CREATE OR REPLACE FUNCTION patch_fn(a int) RETURNS int
LANGUAGE SQL
AS $$ SELECT a + 1 $$;

SELECT patch_fn(1);

CREATE OR REPLACE FUNCTION patch_fn(a int) RETURNS int
LANGUAGE SQL
AS $$ SELECT a + 2 $$;

SELECT patch_fn(1);

-- transactional patch behavior
BEGIN;
CREATE TABLE tx_patch(id int PRIMARY KEY, v text);
INSERT INTO tx_patch VALUES (1, 'x');
ALTER TABLE tx_patch ADD COLUMN extra int DEFAULT 1;
ROLLBACK;
SELECT * FROM tx_patch;

-- bad / validation style cases
ALTER TABLE patch_t
  ADD COLUMN IF NOT EXISTS bad_col int DEFAULT 'x';

CREATE INDEX IF NOT EXISTS patch_t_bad_idx ON patch_t(nope);

ALTER TABLE patch_t
  ADD CONSTRAINT patch_t_status_ck CHECK (status IN ('backfilled', 'ready'));

UPDATE patch_t SET status = 'other' WHERE id = 1;

CREATE OR REPLACE VIEW patch_bad AS SELECT * FROM no_such_table;

CREATE OR REPLACE FUNCTION patch_bad_fn(a int) RETURNS int
LANGUAGE SQL
AS $$ SELECT nope FROM patch_t $$;

DROP SCHEMA compat CASCADE;
