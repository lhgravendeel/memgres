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

CREATE TABLE evo_t(
  id int PRIMARY KEY,
  code text,
  qty text,
  created_on text
);

INSERT INTO evo_t VALUES
(1, 'A', '1', '2024-01-01'),
(2, 'B', '2', '2024-01-02');

ALTER TABLE evo_t ADD COLUMN IF NOT EXISTS active boolean DEFAULT true;
ALTER TABLE evo_t ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE evo_t RENAME COLUMN code TO ext_code;

ALTER TABLE evo_t
  ALTER COLUMN qty TYPE int
  USING qty::int;

ALTER TABLE evo_t
  ALTER COLUMN created_on TYPE date
  USING created_on::date;

CREATE UNIQUE INDEX evo_t_ext_code_uq_idx ON evo_t(ext_code);
ALTER TABLE evo_t
  ADD CONSTRAINT evo_t_ext_code_uq UNIQUE USING INDEX evo_t_ext_code_uq_idx;

ALTER TABLE evo_t DROP COLUMN IF EXISTS updated_at;
ALTER TABLE evo_t ADD COLUMN version int DEFAULT 0 NOT NULL;
ALTER TABLE evo_t ADD COLUMN deleted_at timestamptz;

SELECT * FROM evo_t ORDER BY id;

-- domain evolution in-table
CREATE DOMAIN nonempty_text AS text CHECK (VALUE <> '');
ALTER TABLE evo_t ADD COLUMN tag nonempty_text DEFAULT 'ok';
ALTER DOMAIN nonempty_text ADD CONSTRAINT nonempty_text_len CHECK (char_length(VALUE) <= 10);
ALTER DOMAIN nonempty_text VALIDATE CONSTRAINT nonempty_text_len;

-- enum evolution
CREATE TYPE evo_state AS ENUM ('new', 'active', 'closed');
ALTER TYPE evo_state ADD VALUE 'paused' AFTER 'active';
ALTER TABLE evo_t ADD COLUMN state evo_state DEFAULT 'new';
UPDATE evo_t SET state = 'paused' WHERE id = 1;
SELECT id, ext_code, qty, created_on, tag, state FROM evo_t ORDER BY id;

-- realistic soft-delete/audit shape
ALTER TABLE evo_t ADD COLUMN archived boolean NOT NULL DEFAULT false;
ALTER TABLE evo_t ADD COLUMN updated_by text DEFAULT 'system';

-- bad schema evolution cases
INSERT INTO evo_t(id, ext_code, qty, created_on, version, tag, state, archived, updated_by)
VALUES (3, 'C', 3, DATE '2024-01-03', 0, '', 'new', false, 'u');

ALTER TYPE evo_state ADD VALUE 'active';
ALTER TYPE evo_state ADD VALUE 'later' BEFORE 'missing_label';
ALTER TABLE evo_t DROP COLUMN no_such_col;
ALTER TABLE evo_t ALTER COLUMN qty TYPE int USING note::int;
ALTER TABLE evo_t ADD CONSTRAINT bad_ck CHECK (nope > 0);
ALTER DOMAIN nonempty_text ADD CONSTRAINT bad_dom_ck CHECK (missing_func(VALUE));

DROP SCHEMA compat CASCADE;
