-- ============================================================================
-- A default written for an array column is read by that column's own reader
--
-- The value a column declares as its default is read when the column is
-- defined, by the input function of the column's type. For an array column
-- that reader is the array reader, so a bare word is not a one-element array
-- but a literal the reader cannot start: it must begin with "{" or with
-- dimension information. Only once the braces are read does each element go
-- to the element type's own reader, which is where an element that is not an
-- integer is blamed on integer rather than on the array.
--
-- The same reading settles the default of a column added later and the
-- expression of a stored generated column, since all three are values held to
-- the column they were written for. What the reader accepts it accepts whole:
-- an empty array, a dimensioned literal, whitespace between elements, a NULL
-- element, and the special values a numeric or a float element can spell.
-- Every answer here was read off PostgreSQL 18.
-- ============================================================================

-- setup
CREATE DOMAIN zzt4d_adm AS int;
CREATE TYPE zzt4d_aen AS ENUM ('a','b');

-- ----------------------------------------------------------------------------
-- A bare word is not an array, whatever the array is of
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a text[] DEFAULT 'x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a int[] DEFAULT 'x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a numeric[] DEFAULT 'x');

-- an array of an enum and an array of a domain are read the same way
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a zzt4d_aen[] DEFAULT 'x');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a zzt4d_adm[] DEFAULT 'x');

-- an array of a length-limited varchar too
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a varchar(4)[] DEFAULT 'x');

-- nothing at all is not an array either, nor is whitespace
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: ""
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a text[] DEFAULT '');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "  "
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a text[] DEFAULT '  ');

-- ----------------------------------------------------------------------------
-- Once the braces are read, an element is blamed on the element type
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
CREATE TABLE zzt4d_ad1 (a int[] DEFAULT '{a}');

-- not one of these tables was defined
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM information_schema.tables WHERE table_name = 'zzt4d_ad1';

-- ----------------------------------------------------------------------------
-- What the array reader does accept, it stores
-- ----------------------------------------------------------------------------
CREATE TABLE zzt4d_ad2 (a int[] DEFAULT '{}', b int[] DEFAULT '{1}', c int[] DEFAULT '[1:2]={1,2}', d int[] DEFAULT '{ 1 , 2 }', e int[] DEFAULT '{1,NULL}', f numeric[] DEFAULT '{NaN}', g float8[] DEFAULT '{Infinity}');
INSERT INTO zzt4d_ad2 DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c | d | e | f | g
-- row: {} | {1} | {1,2} | {1,2} | {1,NULL} | {NaN} | {Infinity}
-- end-expected
SELECT a::text AS a, b::text AS b, c::text AS c, d::text AS d, e::text AS e, f::text AS f, g::text AS g FROM zzt4d_ad2;

-- an array of a domain, of an enum and of a length-limited varchar keep their defaults
CREATE TABLE zzt4d_ad3 (a zzt4d_adm[] DEFAULT '{1,2}', b zzt4d_aen[] DEFAULT '{a,b}', c varchar(4)[] DEFAULT '{ab,cd}');
INSERT INTO zzt4d_ad3 DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c
-- row: {1,2} | {a,b} | {ab,cd}
-- end-expected
SELECT a::text AS a, b::text AS b, c::text AS c FROM zzt4d_ad3;

-- ----------------------------------------------------------------------------
-- A column added later is read by the same reader
-- ----------------------------------------------------------------------------
CREATE TABLE zzt4d_ad4 (id int);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
ALTER TABLE zzt4d_ad4 ADD COLUMN a text[] DEFAULT 'x';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
ALTER TABLE zzt4d_ad4 ADD COLUMN b zzt4d_aen[] DEFAULT 'x';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
ALTER TABLE zzt4d_ad4 ADD COLUMN c int[] DEFAULT '{a}';

ALTER TABLE zzt4d_ad4 ADD COLUMN d varchar(4)[] DEFAULT '{ab}';
INSERT INTO zzt4d_ad4 (id) VALUES (1);

-- only the column whose default read as an array is there
-- begin-expected
-- columns: cols
-- row: id,d
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzt4d_ad4';

-- begin-expected
-- columns: d
-- row: {ab}
-- end-expected
SELECT d::text AS d FROM zzt4d_ad4;

-- ----------------------------------------------------------------------------
-- A stored generated column is held to the same reading
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad5 (id int, g text[] GENERATED ALWAYS AS ('x') STORED);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "x"
-- end-expected-error
CREATE TABLE zzt4d_ad6 (id int, g zzt4d_aen[] GENERATED ALWAYS AS ('x') STORED);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
CREATE TABLE zzt4d_ad7 (id int, g int[] GENERATED ALWAYS AS ('{a}') STORED);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzt4d_ad5" does not exist
-- end-expected-error
SELECT * FROM zzt4d_ad5;

CREATE TABLE zzt4d_ad8 (id int, g int[] GENERATED ALWAYS AS ('{1,2}') STORED);
INSERT INTO zzt4d_ad8 (id) VALUES (1);

-- begin-expected
-- columns: g
-- row: {1,2}
-- end-expected
SELECT g::text AS g FROM zzt4d_ad8;

-- cleanup
DROP TABLE zzt4d_ad2;
DROP TABLE zzt4d_ad3;
DROP TABLE zzt4d_ad4;
DROP TABLE zzt4d_ad8;
DROP DOMAIN zzt4d_adm;
DROP TYPE zzt4d_aen;
