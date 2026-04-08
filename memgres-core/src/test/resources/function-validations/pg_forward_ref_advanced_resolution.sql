-- pg_forward_ref_advanced_resolution.sql
--
-- Purpose:
--   Additional advanced cases: schema evolution after first execution,
--   insert/returning with generated values, schema qualification, and
--   partitioned-table / view-like resolution probes.
--
-- Notes:
--   This file avoids creating extra roles, so privilege-oriented security
--   definer cases live in the manual multi-session file.

DROP SCHEMA IF EXISTS test_forward_ref_adv CASCADE;
CREATE SCHEMA test_forward_ref_adv;
SET search_path = test_forward_ref_adv, public;

-------------------------------------------------------------------------------
-- A01: first successful execution followed by ALTER TABLE drop/add behavior
-------------------------------------------------------------------------------
CREATE TABLE queue_servers (
  id integer PRIMARY KEY,
  server_uuid text NOT NULL,
  archived boolean NOT NULL DEFAULT false
);

INSERT INTO queue_servers(id, server_uuid, archived)
VALUES (1, 'one', false), (2, 'two', true);

CREATE OR REPLACE FUNCTION a01_get_uuid_by_id(p_id integer)
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_uuid text;
BEGIN
  SELECT server_uuid INTO v_uuid
  FROM queue_servers
  WHERE id = p_id;
  RETURN v_uuid;
END
$fn$;

-- begin-expected
-- columns: uuid
-- row: one
-- end-expected
SELECT a01_get_uuid_by_id(1) AS uuid;

ALTER TABLE queue_servers DROP COLUMN server_uuid;

-- expect-error: 42703
SELECT a01_get_uuid_by_id(1);

ALTER TABLE queue_servers ADD COLUMN server_uuid text;
UPDATE queue_servers
SET server_uuid = CASE id WHEN 1 THEN 'one-readded' WHEN 2 THEN 'two-readded' END;

-- begin-expected
-- columns: uuid
-- row: one-readded
-- end-expected
SELECT a01_get_uuid_by_id(1) AS uuid;

-------------------------------------------------------------------------------
-- A02: INSERT ... RETURNING with identity / generated expressions
-------------------------------------------------------------------------------
CREATE TABLE work_items (
  task_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  title text NOT NULL,
  title_len integer GENERATED ALWAYS AS (char_length(title)) STORED
);

CREATE OR REPLACE FUNCTION a02_insert_work_item(p_title text)
RETURNS TABLE(task_id integer, title text, title_len integer)
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN QUERY
  INSERT INTO work_items(title)
  VALUES (p_title)
  RETURNING work_items.task_id, work_items.title, work_items.title_len;
END
$fn$;

-- begin-expected
-- columns: task_id,title,title_len
-- row: 1|alpha|5
-- end-expected
SELECT task_id::text AS task_id, title, title_len::text AS title_len
FROM a02_insert_work_item('alpha');

-- begin-expected
-- columns: task_id,title,title_len
-- row: 2|beta|4
-- end-expected
SELECT task_id::text AS task_id, title, title_len::text AS title_len
FROM a02_insert_work_item('beta');

-------------------------------------------------------------------------------
-- A03: qualified names avoid search_path ambiguity
-------------------------------------------------------------------------------
CREATE SCHEMA stable_schema;
CREATE SCHEMA distractor_schema;

CREATE TABLE stable_schema.queue_servers (marker text PRIMARY KEY);
CREATE TABLE distractor_schema.queue_servers (marker text PRIMARY KEY);

INSERT INTO stable_schema.queue_servers(marker) VALUES ('stable');
INSERT INTO distractor_schema.queue_servers(marker) VALUES ('distractor');

CREATE OR REPLACE FUNCTION a03_qualified_lookup()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_marker text;
BEGIN
  SELECT marker INTO v_marker
  FROM stable_schema.queue_servers
  LIMIT 1;
  RETURN v_marker;
END
$fn$;

SET search_path = distractor_schema, test_forward_ref_adv, public;

-- begin-expected
-- columns: marker
-- row: stable
-- end-expected
SELECT a03_qualified_lookup() AS marker;

SET search_path = test_forward_ref_adv, public;

-------------------------------------------------------------------------------
-- A04: function using a future view over partitioned parent table
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION a04_partitioned_view_count()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM a04_future_view;
  RETURN v_count;
END
$fn$;

-- expect-error: 42P01
SELECT a04_partitioned_view_count();

CREATE TABLE a04_events (
  id integer NOT NULL,
  kind text NOT NULL
) PARTITION BY LIST (kind);

CREATE TABLE a04_events_a PARTITION OF a04_events FOR VALUES IN ('a');
CREATE TABLE a04_events_b PARTITION OF a04_events FOR VALUES IN ('b');

INSERT INTO a04_events(id, kind) VALUES (1, 'a'), (2, 'a'), (3, 'b');

CREATE VIEW a04_future_view AS
SELECT * FROM a04_events WHERE kind = 'a';

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT a04_partitioned_view_count()::text AS row_count;

-------------------------------------------------------------------------------
-- A05: exception handling for undefined_column after schema evolution
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION a05_column_exception()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_value text;
BEGIN
  SELECT server_uuid INTO v_value FROM queue_servers WHERE id = 2;
  RETURN v_value;
EXCEPTION
  WHEN undefined_column THEN
    RETURN 'undefined-column';
END
$fn$;

-- begin-expected
-- columns: outcome
-- row: two-readded
-- end-expected
SELECT a05_column_exception() AS outcome;

ALTER TABLE queue_servers DROP COLUMN archived;

-- begin-expected
-- columns: untouched
-- row: two-readded
-- end-expected
SELECT a01_get_uuid_by_id(2) AS untouched;
