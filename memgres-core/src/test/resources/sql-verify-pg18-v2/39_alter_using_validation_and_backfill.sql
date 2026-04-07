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

CREATE TABLE convert_t(
  id int PRIMARY KEY,
  qty_text text,
  flag_text text,
  ref_id int
);

INSERT INTO convert_t VALUES
(1, '1', 'true', 10),
(2, '2', 'false', 20);

-- ALTER COLUMN TYPE ... USING
ALTER TABLE convert_t
  ALTER COLUMN qty_text TYPE int
  USING qty_text::int;

ALTER TABLE convert_t
  ALTER COLUMN flag_text TYPE boolean
  USING flag_text::boolean;

SELECT * FROM convert_t ORDER BY id;

-- backfill then tighten constraints
CREATE TABLE ref_t(id int PRIMARY KEY);
INSERT INTO ref_t VALUES (10), (20);

ALTER TABLE convert_t
  ADD CONSTRAINT convert_t_ref_fk
  FOREIGN KEY (ref_id)
  REFERENCES ref_t(id)
  NOT VALID;

ALTER TABLE convert_t
  VALIDATE CONSTRAINT convert_t_ref_fk;

ALTER TABLE convert_t
  ADD COLUMN note text;

UPDATE convert_t
SET note = CASE
  WHEN qty_text >= 2 THEN 'large'
  ELSE 'small'
END;

ALTER TABLE convert_t
  ALTER COLUMN note SET NOT NULL;

SELECT * FROM convert_t ORDER BY id;

-- UPDATE ... FROM / INSERT ... SELECT / DELETE ... USING backfill patterns
CREATE TABLE src_patch(id int PRIMARY KEY, new_note text);
INSERT INTO src_patch VALUES (1, 'from_src'), (2, 'from_src2');

UPDATE convert_t c
SET note = s.new_note
FROM src_patch s
WHERE c.id = s.id;

CREATE TABLE archive_t AS
SELECT id, qty_text, note FROM convert_t WHERE qty_text >= 1;

SELECT * FROM archive_t ORDER BY id;

DELETE FROM archive_t a
USING convert_t c
WHERE a.id = c.id
  AND c.qty_text = 1;

SELECT * FROM archive_t ORDER BY id;

-- failing conversion / validation cases
INSERT INTO convert_t(id, qty_text, flag_text, ref_id, note)
VALUES (3, 'x', 'not_bool', 99, 'bad');

ALTER TABLE convert_t
  ALTER COLUMN qty_text TYPE int
  USING qty_text::int;

ALTER TABLE convert_t
  ALTER COLUMN flag_text TYPE boolean
  USING flag_text::boolean;

ALTER TABLE convert_t
  ADD CONSTRAINT convert_t_ref_fk2
  FOREIGN KEY (ref_id)
  REFERENCES ref_t(id)
  NOT VALID;

ALTER TABLE convert_t
  VALIDATE CONSTRAINT convert_t_ref_fk2;

DROP SCHEMA compat CASCADE;
