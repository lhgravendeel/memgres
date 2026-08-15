-- ============================================================================
-- An element of an array is held to the type the element was declared with
--
-- varchar(4)[] is an array of varchar(4), not an array of text: the width the
-- element type declares is enforced on every value written into the column,
-- and it is enforced by the element's own type, so the error names character
-- varying(4) rather than the array. character(3)[] pads its elements to three
-- and refuses a fourth character; numeric(4,1)[] rounds an element to one
-- decimal and refuses one that will not fit the precision.
--
-- Every path that writes a row takes the same route, so a literal, an ARRAY
-- constructor, a row read from a query, an UPDATE of the whole array, an
-- UPDATE of one element and a COPY are all held to it, and a row that breaks
-- it is not stored. A column whose array type declared no modifier is
-- untouched: text[] holds elements of any length. An explicit cast to the
-- array type is a conversion rather than a store, and truncates as PostgreSQL
-- has always truncated an explicit cast to varchar(n).
-- Every answer here was read off PostgreSQL 18.
-- ============================================================================

-- setup
CREATE DOMAIN zzt4d_wdv AS varchar(4);
CREATE TABLE zzt4d_wv (a varchar(4)[], t text[]);
CREATE TABLE zzt4d_wc (a char(3)[]);
CREATE TABLE zzt4d_wn (a numeric(4,1)[]);
CREATE TABLE zzt4d_wd (a zzt4d_wdv[]);
INSERT INTO zzt4d_wv (a) VALUES ('{abcd}');

-- ----------------------------------------------------------------------------
-- Every way of writing the array is held to the element's width
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
INSERT INTO zzt4d_wv (a) VALUES ('{abcdefg}');

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
INSERT INTO zzt4d_wv (a) VALUES (ARRAY['abcdefg']);

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
INSERT INTO zzt4d_wv (a) SELECT ARRAY['abcdefg'];

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
UPDATE zzt4d_wv SET a = '{abcdefg}';

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
UPDATE zzt4d_wv SET a = ARRAY['abcdefg'];

-- assigning one element of the array is assigning to the element's type
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
UPDATE zzt4d_wv SET a[1] = 'abcdefg';

-- a row that one element broke was not stored, and the row that was there stands
-- begin-expected
-- columns: a
-- row: {abcd}
-- end-expected
SELECT a::text AS a FROM zzt4d_wv;

-- one overlong element in a multi-row write refuses the whole write
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
INSERT INTO zzt4d_wv (a) VALUES ('{ab}'), ('{abcdefg}');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM zzt4d_wv;

-- ----------------------------------------------------------------------------
-- A column that declared no modifier is not held to one
-- ----------------------------------------------------------------------------
INSERT INTO zzt4d_wv (t) VALUES ('{abcdefg}');

-- begin-expected
-- columns: t
-- row: {abcdefg}
-- end-expected
SELECT t::text AS t FROM zzt4d_wv WHERE t IS NOT NULL;

-- ----------------------------------------------------------------------------
-- A character element is padded to its width and refused past it
-- ----------------------------------------------------------------------------
INSERT INTO zzt4d_wc VALUES ('{ab}');

-- begin-expected
-- columns: a
-- row: {"ab "}
-- end-expected
SELECT a::text AS a FROM zzt4d_wc;

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character(3)
-- end-expected-error
INSERT INTO zzt4d_wc VALUES ('{abcd}');

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character(3)
-- end-expected-error
UPDATE zzt4d_wc SET a = '{abcd}';

-- ----------------------------------------------------------------------------
-- A numeric element is rounded to its scale and refused past its precision
-- ----------------------------------------------------------------------------
INSERT INTO zzt4d_wn VALUES ('{123.45}');

-- begin-expected
-- columns: a
-- row: {123.5}
-- end-expected
SELECT a::text AS a FROM zzt4d_wn;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
INSERT INTO zzt4d_wn VALUES ('{12345.4}');

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
UPDATE zzt4d_wn SET a = '{12345.4}';

-- ----------------------------------------------------------------------------
-- An element whose type is a domain carries the width of the domain's base
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(4)
-- end-expected-error
INSERT INTO zzt4d_wd VALUES ('{abcdefg}');

INSERT INTO zzt4d_wd VALUES ('{abcd}');

-- begin-expected
-- columns: a
-- row: {abcd}
-- end-expected
SELECT a::text AS a FROM zzt4d_wd;

-- ----------------------------------------------------------------------------
-- An explicit cast to the array type converts rather than refuses
-- ----------------------------------------------------------------------------
-- begin-expected
-- columns: v
-- row: {abcd}
-- end-expected
SELECT (ARRAY['abcdefg']::varchar(4)[])::text AS v;

-- begin-expected
-- columns: v
-- row: {abc}
-- end-expected
SELECT (ARRAY['abcdefg']::char(3)[])::text AS v;

-- cleanup
DROP TABLE zzt4d_wv;
DROP TABLE zzt4d_wc;
DROP TABLE zzt4d_wn;
DROP TABLE zzt4d_wd;
DROP DOMAIN zzt4d_wdv;
