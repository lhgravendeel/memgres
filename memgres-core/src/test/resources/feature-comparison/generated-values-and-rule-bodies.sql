-- A generated column is computed from its expression against the row's values, so a value that
-- spells another column's name is a value and nothing is read twice.
DROP TABLE IF EXISTS zzw3a_gc1 CASCADE;
CREATE TABLE zzw3a_gc1 (a text, b text, g text GENERATED ALWAYS AS (a || b) STORED);
INSERT INTO zzw3a_gc1 (a, b) VALUES ('b', 'x');

-- begin-expected
-- columns: g
-- row: bx
-- end-expected
SELECT g FROM zzw3a_gc1;

-- begin-expected
-- columns: len
-- row: 2
-- end-expected
SELECT length(g) AS len FROM zzw3a_gc1;

-- begin-expected
-- columns: isnull
-- row: f
-- end-expected
SELECT g IS NULL AS isnull FROM zzw3a_gc1;

DROP TABLE zzw3a_gc1;

-- The generation expression's own string literals are part of the expression: a column name
-- spelt inside one of them is text, not a reference.
DROP TABLE IF EXISTS zzw3a_gc2 CASCADE;
DROP TABLE IF EXISTS zzw3a_gc2b CASCADE;
DROP TABLE IF EXISTS zzw3a_gc2c CASCADE;
CREATE TABLE zzw3a_gc2 (n int, s text GENERATED ALWAYS AS (n::text || 'n') STORED);
INSERT INTO zzw3a_gc2 (n) VALUES (4);

-- begin-expected
-- columns: s
-- row: 4n
-- end-expected
SELECT s FROM zzw3a_gc2;

CREATE TABLE zzw3a_gc2b (a text, b text, g text GENERATED ALWAYS AS (coalesce(a, 'b') || coalesce(b, 'a')) STORED);
INSERT INTO zzw3a_gc2b (a, b) VALUES (NULL, NULL);

-- begin-expected
-- columns: g
-- row: ba
-- end-expected
SELECT g FROM zzw3a_gc2b;

CREATE TABLE zzw3a_gc2c (a text, g text GENERATED ALWAYS AS (a || 'a') STORED);
INSERT INTO zzw3a_gc2c (a) VALUES ('M');

-- begin-expected
-- columns: g
-- row: Ma
-- end-expected
SELECT g FROM zzw3a_gc2c;

DROP TABLE zzw3a_gc2;
DROP TABLE zzw3a_gc2b;
DROP TABLE zzw3a_gc2c;

-- A value is carried to the generation expression as it is: a backslash stays a backslash and a
-- dollar sign followed by a digit is two characters.
DROP TABLE IF EXISTS zzw3a_gc3 CASCADE;
CREATE TABLE zzw3a_gc3 (a text, g text GENERATED ALWAYS AS (a || '!') STORED);
INSERT INTO zzw3a_gc3 (a) VALUES ('p$1q');
INSERT INTO zzw3a_gc3 (a) VALUES ('back\slash');

-- begin-expected
-- columns: g
-- row: back\slash!
-- row: p$1q!
-- end-expected
SELECT g FROM zzw3a_gc3 ORDER BY g;

DROP TABLE zzw3a_gc3;

-- Values with no literal spelling of their own -- a bytea, a timestamp, a date, an array -- are
-- read by the generation expression as the values they are.
DROP TABLE IF EXISTS zzw3a_gc4 CASCADE;
CREATE TABLE zzw3a_gc4 (b bytea, t timestamp, d date, arr int[], l int GENERATED ALWAYS AS (length(b)) STORED, u timestamp GENERATED ALWAYS AS (t + interval '1 day') STORED, y int GENERATED ALWAYS AS (extract(year from d)) STORED, n int GENERATED ALWAYS AS (array_length(arr, 1)) STORED);
INSERT INTO zzw3a_gc4 (b, t, d, arr) VALUES ('\x010203'::bytea, '2020-01-01 10:00', '2020-03-04', '{7,8,9}');

-- begin-expected
-- columns: l
-- row: 3
-- end-expected
SELECT l FROM zzw3a_gc4;

-- begin-expected
-- columns: u
-- row: 2020-01-02 10:00:00
-- end-expected
SELECT u::text AS u FROM zzw3a_gc4;

-- begin-expected
-- columns: y
-- row: 2020
-- end-expected
SELECT y FROM zzw3a_gc4;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT n FROM zzw3a_gc4;

DROP TABLE zzw3a_gc4;

-- Every row computes its own generated key, and a generation expression that raises is the
-- statement's error rather than a value written in its place.
DROP TABLE IF EXISTS zzw3a_gc5 CASCADE;
DROP TABLE IF EXISTS zzw3a_gc6 CASCADE;
CREATE TABLE zzw3a_gc5 (a text, b text, g text GENERATED ALWAYS AS (a || b) STORED PRIMARY KEY);
INSERT INTO zzw3a_gc5 (a, b) VALUES ('b', 'x');
INSERT INTO zzw3a_gc5 (a, b) VALUES ('b', 'y');

-- begin-expected
-- columns: g
-- row: bx
-- row: by
-- end-expected
SELECT g FROM zzw3a_gc5 ORDER BY g;

CREATE TABLE zzw3a_gc6 (a int, g int GENERATED ALWAYS AS (100 / a) STORED);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO zzw3a_gc6 (a) VALUES (0);

-- begin-expected
-- columns: rows_written
-- row: 0
-- end-expected
SELECT count(*) AS rows_written FROM zzw3a_gc6;

DROP TABLE zzw3a_gc5;
DROP TABLE zzw3a_gc6;

-- Table data cannot become syntax: a text value holding a subquery is concatenated, not run.
DROP TABLE IF EXISTS zzw3a_gc7 CASCADE;
DROP TABLE IF EXISTS zzw3a_gc7src CASCADE;
CREATE TABLE zzw3a_gc7src (a text);
INSERT INTO zzw3a_gc7src VALUES ('Q');
CREATE TABLE zzw3a_gc7 (a text, b text, g text GENERATED ALWAYS AS (a || b) STORED);
INSERT INTO zzw3a_gc7 (a, b) VALUES ('b', ' || (SELECT count(*) FROM zzw3a_gc7src) || ');

-- begin-expected
-- columns: verbatim
-- row: t
-- end-expected
SELECT g = 'b' || ' || (SELECT count(*) FROM zzw3a_gc7src) || ' AS verbatim FROM zzw3a_gc7;

-- begin-expected
-- columns: starts
-- row: b
-- end-expected
SELECT left(g, 1) AS starts FROM zzw3a_gc7;

DROP TABLE zzw3a_gc7;
DROP TABLE zzw3a_gc7src;

-- A rule action's own string literals are left alone, on the INSERT path and on the UPDATE one:
-- NEW. and OLD. written inside one of them are part of that string.
DROP TABLE IF EXISTS zzw3a_ru1 CASCADE;
DROP TABLE IF EXISTS zzw3a_ru1log CASCADE;
CREATE TABLE zzw3a_ru1 (a text);
CREATE TABLE zzw3a_ru1log (what text);
CREATE RULE zzw3a_ru1r AS ON INSERT TO zzw3a_ru1 DO ALSO INSERT INTO zzw3a_ru1log VALUES ('NEW.a is the name');
INSERT INTO zzw3a_ru1 (a) VALUES ('V');

-- begin-expected
-- columns: what
-- row: NEW.a is the name
-- end-expected
SELECT what FROM zzw3a_ru1log;

-- begin-expected
-- columns: a
-- row: V
-- end-expected
SELECT a FROM zzw3a_ru1;

DROP TABLE zzw3a_ru1 CASCADE;
DROP TABLE zzw3a_ru1log;

DROP TABLE IF EXISTS zzw3a_ru2 CASCADE;
DROP TABLE IF EXISTS zzw3a_ru2log CASCADE;
CREATE TABLE zzw3a_ru2 (a text);
CREATE TABLE zzw3a_ru2log (what text);
INSERT INTO zzw3a_ru2 VALUES ('one');
CREATE RULE zzw3a_ru2r AS ON UPDATE TO zzw3a_ru2 DO ALSO INSERT INTO zzw3a_ru2log VALUES ('OLD.a means the old row');
UPDATE zzw3a_ru2 SET a = 'two';

-- begin-expected
-- columns: a
-- row: two
-- end-expected
SELECT a FROM zzw3a_ru2;

-- begin-expected
-- columns: what
-- row: OLD.a means the old row
-- end-expected
SELECT what FROM zzw3a_ru2log;

DROP TABLE zzw3a_ru2 CASCADE;
DROP TABLE zzw3a_ru2log;

-- An INSERT rule reads NEW as values: a bytea is bytes, a timestamp is a timestamp and an array
-- is an array, whatever printing them would have made of them.
DROP TABLE IF EXISTS zzw3a_ru3 CASCADE;
DROP TABLE IF EXISTS zzw3a_ru3log CASCADE;
CREATE TABLE zzw3a_ru3 (b bytea, t timestamp, arr int[]);
CREATE TABLE zzw3a_ru3log (h text, ts text, n int);
CREATE RULE zzw3a_ru3r AS ON INSERT TO zzw3a_ru3 DO ALSO INSERT INTO zzw3a_ru3log VALUES (encode(NEW.b, 'hex'), NEW.t::text, array_length(NEW.arr, 1));
INSERT INTO zzw3a_ru3 (b, t, arr) VALUES ('\x0a0b'::bytea, '2021-05-06 07:08:09', '{1,2}');

-- begin-expected
-- columns: h
-- row: 0a0b
-- end-expected
SELECT h FROM zzw3a_ru3log;

-- begin-expected
-- columns: ts
-- row: 2021-05-06 07:08:09
-- end-expected
SELECT ts FROM zzw3a_ru3log;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT n FROM zzw3a_ru3log;

DROP TABLE zzw3a_ru3 CASCADE;
DROP TABLE zzw3a_ru3log;

-- A value that spells a row reference is a value: it does not re-enter the rule body, and the
-- old row and the new one are told apart by which relation they are read from.
DROP TABLE IF EXISTS zzw3a_ru4 CASCADE;
DROP TABLE IF EXISTS zzw3a_ru4log CASCADE;
CREATE TABLE zzw3a_ru4 (a text, b text);
CREATE TABLE zzw3a_ru4log (x text, y text);
CREATE RULE zzw3a_ru4r AS ON INSERT TO zzw3a_ru4 DO ALSO INSERT INTO zzw3a_ru4log VALUES (NEW.a, NEW.b);
INSERT INTO zzw3a_ru4 (a, b) VALUES ('NEW.b', 'z');

-- begin-expected
-- columns: logged
-- row: NEW.b/z
-- end-expected
SELECT x || '/' || y AS logged FROM zzw3a_ru4log;

-- begin-expected
-- columns: written
-- row: NEW.b/z
-- end-expected
SELECT a || '/' || b AS written FROM zzw3a_ru4;

DROP TABLE zzw3a_ru4 CASCADE;
DROP TABLE zzw3a_ru4log;

DROP TABLE IF EXISTS zzw3a_ru5 CASCADE;
DROP TABLE IF EXISTS zzw3a_ru5log CASCADE;
CREATE TABLE zzw3a_ru5 (a text, b text);
CREATE TABLE zzw3a_ru5log (x text);
CREATE RULE zzw3a_ru5r AS ON UPDATE TO zzw3a_ru5 DO ALSO INSERT INTO zzw3a_ru5log VALUES (OLD.a || '/' || NEW.a);
INSERT INTO zzw3a_ru5 VALUES ('OLD.b', 'k');
UPDATE zzw3a_ru5 SET a = 'NEW.b';

-- begin-expected
-- columns: x
-- row: OLD.b/NEW.b
-- end-expected
SELECT x FROM zzw3a_ru5log;

DROP TABLE zzw3a_ru5 CASCADE;
DROP TABLE zzw3a_ru5log;