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

CREATE DOMAIN posint AS int CHECK (VALUE > 0);
CREATE DOMAIN shorttxt AS text DEFAULT 'x' CHECK (char_length(VALUE) <= 3);
CREATE DOMAIN posint_array AS int[] CHECK (array_length(VALUE, 1) > 0);

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE addr AS (
  street text,
  zip int
);
CREATE TYPE contact AS (
  name text,
  home addr
);

CREATE TABLE people(
  id int PRIMARY KEY,
  name shorttxt NOT NULL,
  age posint,
  moods mood[],
  home addr,
  info contact,
  lucky posint_array
);

INSERT INTO people VALUES
(1, 'ann', 30, ARRAY['ok'::mood, 'happy'::mood], ROW('Main', 12345), ROW('Ann', ROW('Main', 12345)), ARRAY[7,8]),
(2, DEFAULT, 5, ARRAY['sad'::mood], ROW('Side', 54321), ROW('Bo', ROW('Side', 54321)), ARRAY[1]);

SELECT id, name, age, moods, home, info, lucky FROM people ORDER BY id;
SELECT home.street, home.zip, info.home.street, info.home.zip FROM people ORDER BY id;
SELECT (ROW('X', 10000)::addr).street, (ROW('Y', ROW('Road', 20000))::contact).home.zip;
SELECT ROW(1,'x') = ROW(1,'x'), ROW(1,'x') < ROW(2,'a');
SELECT ARRAY[ROW('Main', 12345)::addr, ROW('Side', 54321)::addr];
SELECT pg_typeof(ARRAY[ROW('Main', 12345)::addr, ROW('Side', 54321)::addr]);
SELECT unnest(moods) FROM people ORDER BY 1;

ALTER TYPE mood ADD VALUE 'great';
SELECT 'great'::mood;
SELECT 'sad'::mood < 'ok'::mood, 'ok'::mood < 'happy'::mood;

-- more composite/row expansion
CREATE FUNCTION get_addr(i int) RETURNS addr
LANGUAGE SQL
AS $$ SELECT home FROM people WHERE id = i $$;

SELECT (get_addr(1)).street, (get_addr(1)).zip;
SELECT (people.home).* FROM people ORDER BY id;

-- bad domain/enum/composite cases
INSERT INTO people(id, name, age, moods, home, info, lucky)
VALUES (3, 'toolong', 1, ARRAY['ok'::mood], ROW('Road', 1), ROW('C', ROW('Road', 1)), ARRAY[1]);

INSERT INTO people(id, name, age, moods, home, info, lucky)
VALUES (4, 'ed', 0, ARRAY['ok'::mood], ROW('Road', 1), ROW('D', ROW('Road', 1)), ARRAY[1]);

INSERT INTO people(id, name, age, moods, home, info, lucky)
VALUES (5, 'ed', 1, ARRAY['nope'::mood], ROW('Road', 1), ROW('E', ROW('Road', 1)), ARRAY[1]);

INSERT INTO people(id, name, age, moods, home, info, lucky)
VALUES (6, 'ed', 1, ARRAY['ok'::mood], ROW('Road', 1), ROW('F', ROW('Road', 1)), ARRAY[]::int[]);

SELECT ROW('x', 1, 2)::addr;
SELECT (ROW('Main', 12345)::addr).nope;
SELECT ARRAY['sad'::mood, 'nope'::mood];
SELECT 'missing'::mood;
ALTER TYPE mood ADD VALUE 'ok';

DROP SCHEMA compat CASCADE;
