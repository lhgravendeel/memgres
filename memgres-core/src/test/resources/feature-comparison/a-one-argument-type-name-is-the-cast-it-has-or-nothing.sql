-- A type name written as a one-argument call is a cast, and only where the cast exists.
--
-- PostgreSQL reads typename(x) as a coercion only when pg_cast (plus the two general rules -- a
-- type casts to itself, and an explicit cast may go through the type's text form when either side
-- is a string type) says the conversion is there. Where it is not, the name falls through to the
-- routines of that name and the call is 42883 "function date(integer) does not exist" with the
-- standard hint.
--
-- memgres used to read every one-argument type-name call as a cast, so CHECK (date(a)) over an
-- integer column defined a constraint PostgreSQL refuses outright, and the same call answered a
-- value at run time. The refusal has to be exact in both places -- the message names the argument
-- by the type it was written with -- and every conversion that does exist has to keep working,
-- which is the larger half of this file.

-- setup
DROP TABLE IF EXISTS zzt4c_ok1, zzt4c_ok2, zzt4c_ok3, zzt4c_ok4, zzt4c_ok5, zzt4c_ok6 CASCADE;
DROP TABLE IF EXISTS zzt4c_ok7, zzt4c_ok8, zzt4c_ok9, zzt4c_oka, zzt4c_okb, zzt4c_okc CASCADE;
DROP TABLE IF EXISTS zzt4c_num CASCADE;

-- ---------------------------------------------------------------------------
-- 1. A constraint naming a conversion that does not exist is refused as it is defined
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function date(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (date(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function uuid(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (uuid(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function inet(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (inet(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function jsonb(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (jsonb(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function xml(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (xml(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function macaddr(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (macaddr(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function tsvector(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (tsvector(a) IS NOT NULL));

-- The geometric constructors take shapes, not a number.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function point(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (point(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function box(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (box(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function circle(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (circle(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function polygon(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (polygon(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lseg(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (lseg(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function path(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (path(a) IS NOT NULL));

-- A multirange is built from its range, and from nothing else.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4multirange(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (int4multirange(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function timestamptz(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (timestamptz(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function timetz(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (timetz(a) IS NOT NULL));

-- It is the type of the expression that is judged, not the column it was read from.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function date(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a int, CHECK (date(a + 1) IS NOT NULL));

-- int4(boolean) is a cast PostgreSQL makes; int8(boolean) is not, and the two are told apart.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int8(boolean) does not exist
-- end-expected-error
CREATE TABLE zzt4c_bad (a bool, CHECK (int8(a) IS NOT NULL));

-- Nothing above got as far as a relation.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzt4c_bad';

-- ---------------------------------------------------------------------------
-- 2. Every conversion that does exist still defines its constraint
-- ---------------------------------------------------------------------------

CREATE TABLE zzt4c_ok1 (a int,
    CHECK (text(a) IS NOT NULL), CHECK (int8(a) IS NOT NULL), CHECK (float8(a) IS NOT NULL),
    CHECK (bool(a) IS NOT NULL), CHECK (oid(a) IS NOT NULL), CHECK (money(a) IS NOT NULL),
    CHECK (bytea(a) IS NOT NULL), CHECK (name(a) IS NOT NULL), CHECK (regclass(a) IS NOT NULL),
    CHECK (int4(a) IS NOT NULL), CHECK (float4(a) IS NOT NULL), CHECK (int2(a) IS NOT NULL));

CREATE TABLE zzt4c_ok2 (a bool, CHECK (int4(a) IS NOT NULL));
CREATE TABLE zzt4c_ok3 (a bigint, CHECK (int4(a) IS NOT NULL));
CREATE TABLE zzt4c_ok4 (a numeric, CHECK (int8(a) IS NOT NULL));
CREATE TABLE zzt4c_ok5 (a json, CHECK (jsonb(a) IS NOT NULL));
CREATE TABLE zzt4c_ok6 (a cidr, CHECK (inet(a) IS NOT NULL));
CREATE TABLE zzt4c_ok7 (a timestamp, CHECK (date(a) IS NOT NULL));
CREATE TABLE zzt4c_ok8 (a text, CHECK (date(a) IS NOT NULL));
CREATE TABLE zzt4c_ok9 (a date, CHECK (timestamptz(a) IS NOT NULL));
CREATE TABLE zzt4c_oka (a box, CHECK (point(a) IS NOT NULL));
CREATE TABLE zzt4c_okb (a numrange, CHECK (nummultirange(a) IS NOT NULL));

-- A type casts to itself, so the name of the column's own type is always a call it can make.
CREATE TABLE zzt4c_okc (a date, CHECK (date(a) IS NOT NULL));

-- begin-expected
-- columns: n
-- row: 12
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzt4c_ok%';

-- And the constraint the definition kept is the constraint that is enforced.
CREATE TABLE zzt4c_num (a int, CONSTRAINT zzt4c_num_ck CHECK (int4(a) > 0));
INSERT INTO zzt4c_num VALUES (1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "zzt4c_num_ck"
-- end-expected-error
INSERT INTO zzt4c_num VALUES (-1);

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT a FROM zzt4c_num ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 3. The same refusal where the call is written to run
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function date(integer) does not exist
-- end-expected-error
SELECT date(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function uuid(integer) does not exist
-- end-expected-error
SELECT uuid(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function inet(integer) does not exist
-- end-expected-error
SELECT inet(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function jsonb(integer) does not exist
-- end-expected-error
SELECT jsonb(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function xml(integer) does not exist
-- end-expected-error
SELECT xml(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function macaddr(integer) does not exist
-- end-expected-error
SELECT macaddr(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function tsvector(integer) does not exist
-- end-expected-error
SELECT tsvector(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function timestamptz(integer) does not exist
-- end-expected-error
SELECT timestamptz(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function timetz(integer) does not exist
-- end-expected-error
SELECT timetz(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int8(boolean) does not exist
-- end-expected-error
SELECT int8(true);

-- The argument is named by the type it was written with: a bigint is not an integer.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function date(bigint) does not exist
-- end-expected-error
SELECT date(1::bigint);

-- ---------------------------------------------------------------------------
-- 4. The conversions that do exist answer, and answer in their own type
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: a | b | c | d
-- row: 2|1|1|1
-- end-expected
SELECT int4(1.9) AS a, text(1) AS b, int8(1) AS c, float8(1) AS d;

-- begin-expected
-- columns: a | b | c | d | e
-- row: t|1|1|1|1.5
-- end-expected
SELECT bool(1) AS a, oid(1) AS b, name(1) AS c, int2(1) AS d, float4(1.5) AS e;

-- begin-expected
-- columns: a | b
-- row: {"a": 1}|1.2.3.0/24
-- end-expected
SELECT jsonb('{"a": 1}'::json)::text AS a, inet('1.2.3.0/24'::cidr)::text AS b;

-- begin-expected
-- columns: a | b
-- row: 2020-01-02|2020-01-02
-- end-expected
SELECT date('2020-01-02'::text) AS a, date(TIMESTAMP '2020-01-02 03:04:05') AS b;

-- begin-expected
-- columns: a | b | c
-- row: 1|2|1
-- end-expected
SELECT int4(1::bigint) AS a, int8(1.5::numeric) AS b, int4(true) AS c;

-- begin-expected
-- columns: a
-- row: 11111111-1111-1111-1111-111111111111
-- end-expected
SELECT uuid('11111111-1111-1111-1111-111111111111'::text) AS a;

-- begin-expected
-- columns: a | b
-- row: (0.5,0.5)|{[1.5,5.5)}
-- end-expected
SELECT point(box '((0,0),(1,1))')::text AS a, nummultirange(numrange(1.5, 5.5))::text AS b;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT timestamptz(DATE '2020-01-02') = TIMESTAMPTZ '2020-01-02 00:00:00' AS a;

-- cleanup
DROP TABLE IF EXISTS zzt4c_ok1, zzt4c_ok2, zzt4c_ok3, zzt4c_ok4, zzt4c_ok5, zzt4c_ok6 CASCADE;
DROP TABLE IF EXISTS zzt4c_ok7, zzt4c_ok8, zzt4c_ok9, zzt4c_oka, zzt4c_okb, zzt4c_okc CASCADE;
DROP TABLE IF EXISTS zzt4c_num CASCADE;
