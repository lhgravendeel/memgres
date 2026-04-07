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

CREATE TABLE parent(
  id int PRIMARY KEY,
  code text UNIQUE NOT NULL,
  qty int CHECK (qty >= 0),
  note text,
  created_id bigint GENERATED ALWAYS AS IDENTITY
);

CREATE TABLE child(
  id int PRIMARY KEY,
  parent_id int REFERENCES parent(id),
  qty int NOT NULL CHECK (qty > 0),
  total int GENERATED ALWAYS AS (qty * 10) STORED
);

INSERT INTO parent(id, code, qty, note) VALUES
(1, 'A', 5, 'ok'),
(2, 'B', 0, NULL);

INSERT INTO child(id, parent_id, qty) VALUES
(10, 1, 2),
(11, 2, 3);

SELECT * FROM parent ORDER BY id;
SELECT id, parent_id, qty, total, pg_typeof(total) FROM child ORDER BY id;

-- constraint failures
INSERT INTO parent(id, code, qty) VALUES (1, 'C', 1);
INSERT INTO parent(id, code, qty) VALUES (3, 'A', 1);
INSERT INTO parent(id, code, qty) VALUES (4, NULL, 1);
INSERT INTO parent(id, code, qty) VALUES (5, 'E', -1);
INSERT INTO child(id, parent_id, qty) VALUES (12, 99, 1);
INSERT INTO child(id, parent_id, qty) VALUES (13, 1, 0);
INSERT INTO child(id, parent_id, qty, total) VALUES (14, 1, 2, 999);
UPDATE parent SET code = 'B' WHERE id = 1;
UPDATE child SET parent_id = 999 WHERE id = 10;
UPDATE child SET qty = NULL WHERE id = 10;

-- identity / generated oddities
INSERT INTO parent(created_id, id, code, qty) VALUES (999, 6, 'F', 1);
UPDATE child SET total = 999 WHERE id = 10;

-- exclusion basics
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE booking(
  room int,
  during tsrange,
  EXCLUDE USING gist (room WITH =, during WITH &&)
);
INSERT INTO booking VALUES (1, tsrange('2024-01-01 10:00','2024-01-01 11:00'));
INSERT INTO booking VALUES (1, tsrange('2024-01-01 11:00','2024-01-01 12:00'));
INSERT INTO booking VALUES (1, tsrange('2024-01-01 10:30','2024-01-01 10:45'));

DROP SCHEMA compat CASCADE;



-- more FK actions and deferrable constraints
CREATE TABLE parent2(
  id int PRIMARY KEY,
  code text UNIQUE
);
CREATE TABLE child2(
  id int PRIMARY KEY,
  parent_id int,
  parent_code text,
  CONSTRAINT child2_fk_parent_id
    FOREIGN KEY (parent_id)
    REFERENCES parent2(id)
    DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT child2_fk_parent_code
    FOREIGN KEY (parent_code)
    REFERENCES parent2(code)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

INSERT INTO parent2 VALUES (1, 'AA'), (2, 'BB');
BEGIN;
INSERT INTO child2 VALUES (1, 1, 'AA');
INSERT INTO child2 VALUES (2, 99, 'AA');
COMMIT;
ROLLBACK;

INSERT INTO child2 VALUES (3, 1, 'AA');
UPDATE parent2 SET code = 'CC' WHERE id = 1;
SELECT * FROM child2 ORDER BY id;
DELETE FROM parent2 WHERE id = 1;
SELECT * FROM child2 ORDER BY id;

-- self-reference and multi-column uniqueness
CREATE TABLE selfref(
  id int PRIMARY KEY,
  parent_id int REFERENCES selfref(id),
  a int,
  b int,
  UNIQUE (a, b)
);
INSERT INTO selfref VALUES (1, NULL, 1, 1);
INSERT INTO selfref VALUES (2, 1, 1, 2);
INSERT INTO selfref VALUES (3, 99, 1, 3);
INSERT INTO selfref VALUES (4, 1, 1, 2);

