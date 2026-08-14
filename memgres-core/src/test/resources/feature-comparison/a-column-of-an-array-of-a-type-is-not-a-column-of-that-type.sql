-- ============================================================================
-- A column of an array of a written type, and a column of a domain built over
-- an array, are not the same column
--
-- Both record the same pair -- the name of a user-written type and an array
-- beside it -- and only one question tells them apart: is that name the
-- column's own type, or its element's? A column declared dm[] is a column of
-- an array whose elements are the domain dm; a column declared da, where da
-- is a domain over int[], is a column of da itself and its elements are plain
-- integers.
--
-- Every answer a client reads about such a column follows from that question:
-- what pg_typeof says the column is, what information_schema.columns
-- publishes as its data type and its underlying type, what pg_attribute says
-- about its length, storage and pass-by-value, and what type one element or a
-- slice of elements has. An array is always varlena, however small the type
-- its elements are, so an array of an enum is -1/x/f and not the enum's own
-- 4/p/t. Every answer here was read off PostgreSQL 18.
-- ============================================================================

-- setup
CREATE DOMAIN zzt4d_pdm AS int;
CREATE TYPE zzt4d_pen AS ENUM ('a','b');
CREATE TYPE zzt4d_pcp AS (x int);
CREATE TYPE zzt4d_prg AS RANGE (subtype = int4);
CREATE DOMAIN zzt4d_pda AS int[];
CREATE TABLE zzt4d_pt (a zzt4d_pdm[], b zzt4d_pen[], c zzt4d_pcp[], r zzt4d_prg[], d zzt4d_pda);
INSERT INTO zzt4d_pt VALUES ('{1,2}', '{a,b}', ARRAY[ROW(1)::zzt4d_pcp], ARRAY['[1,3)'::zzt4d_prg], '{7,8}');

-- ----------------------------------------------------------------------------
-- What the column is
-- ----------------------------------------------------------------------------
-- begin-expected
-- columns: a | b | c | r | d
-- row: zzt4d_pdm[] | zzt4d_pen[] | zzt4d_pcp[] | zzt4d_prg[] | zzt4d_pda
-- end-expected
SELECT pg_typeof(a)::text AS a, pg_typeof(b)::text AS b, pg_typeof(c)::text AS c, pg_typeof(r)::text AS r, pg_typeof(d)::text AS d FROM zzt4d_pt;

-- ----------------------------------------------------------------------------
-- What the catalogue publishes it as
-- ----------------------------------------------------------------------------
-- an array of a written type is an ARRAY of that type's array type, whichever
-- kind of type was written; the domain over an array is an ARRAY of int4,
-- because that is what the domain is over
-- begin-expected
-- columns: column_name | data_type | udt_schema | udt_name
-- row: a | ARRAY | public | _zzt4d_pdm
-- row: b | ARRAY | public | _zzt4d_pen
-- row: c | ARRAY | public | _zzt4d_pcp
-- row: r | ARRAY | public | _zzt4d_prg
-- row: d | ARRAY | pg_catalog | _int4
-- end-expected
SELECT column_name, data_type, udt_schema, udt_name FROM information_schema.columns WHERE table_name = 'zzt4d_pt' ORDER BY ordinal_position;

-- ----------------------------------------------------------------------------
-- The layout pg_attribute gives it is an array's layout
-- ----------------------------------------------------------------------------
-- begin-expected
-- columns: attname | typ | attlen | attstorage | attbyval | attndims
-- row: a | zzt4d_pdm[] | -1 | x | false | 1
-- row: b | zzt4d_pen[] | -1 | x | false | 1
-- row: c | zzt4d_pcp[] | -1 | x | false | 1
-- row: r | zzt4d_prg[] | -1 | x | false | 1
-- end-expected
SELECT attname, atttypid::regtype::text AS typ, attlen, attstorage::text AS attstorage, attbyval, attndims FROM pg_attribute WHERE attrelid = 'zzt4d_pt'::regclass AND attnum > 0 AND attname <> 'd' ORDER BY attnum;

-- the column of the domain points at the domain and is varlena like the array it is over
-- begin-expected
-- columns: attname | typ | attlen | attstorage | attbyval
-- row: d | zzt4d_pda | -1 | x | false
-- end-expected
SELECT attname, atttypid::regtype::text AS typ, attlen, attstorage::text AS attstorage, attbyval FROM pg_attribute WHERE attrelid = 'zzt4d_pt'::regclass AND attname = 'd';

-- and format_type prints the column's own type either way
-- begin-expected
-- columns: a | d
-- row: zzt4d_pdm[] | zzt4d_pda
-- end-expected
SELECT format_type(a.atttypid, NULL) AS a, format_type(d.atttypid, NULL) AS d FROM pg_attribute a, pg_attribute d WHERE a.attrelid = 'zzt4d_pt'::regclass AND a.attname = 'a' AND d.attrelid = 'zzt4d_pt'::regclass AND d.attname = 'd';

-- ----------------------------------------------------------------------------
-- What one element, and a slice of them, is
-- ----------------------------------------------------------------------------
-- an element of an array of a written type is of that type; an element of the
-- domain over int[] is an integer; a slice of an array is still that array
-- begin-expected
-- columns: e1 | e2 | e3 | e4 | e5 | e6
-- row: zzt4d_pdm | zzt4d_pen | zzt4d_pdm[] | zzt4d_pcp | zzt4d_prg | integer
-- end-expected
SELECT pg_typeof(a[1])::text AS e1, pg_typeof(b[1])::text AS e2, pg_typeof(a[1:2])::text AS e3, pg_typeof(c[1])::text AS e4, pg_typeof(r[1])::text AS e5, pg_typeof(d[1])::text AS e6 FROM zzt4d_pt;

-- and reads back as the value it stored
-- begin-expected
-- columns: a | b | d | e1 | e2 | e3
-- row: {1,2} | {a,b} | {7,8} | 1 | a | 7
-- end-expected
SELECT a::text AS a, b::text AS b, d::text AS d, a[1]::text AS e1, b[1]::text AS e2, d[1]::text AS e3 FROM zzt4d_pt;

-- cleanup
DROP TABLE zzt4d_pt;
DROP DOMAIN zzt4d_pda;
DROP DOMAIN zzt4d_pdm;
DROP TYPE zzt4d_pen;
DROP TYPE zzt4d_pcp;
DROP TYPE zzt4d_prg;
