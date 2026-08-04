-- A type name written like a function call, which PostgreSQL reads as a cast.
--
-- PostgreSQL resolves typename(x) as a coercion whenever no pg_proc row of that name matches
-- (parse_func.c, FUNCDETAIL_COERCION), for every type whose name is a legal function name --
-- built-in, a user's domain, a user's enum. memgres implemented that for a handful of names and
-- answered 42883 "function int4(integer) does not exist" for the rest, so ordinary SQL an
-- application writes -- int4('42'), bool('t'), date(now()), uuid(...), jsonb(...), tsvector(...)
-- -- was refused outright while CAST(... AS int4) beside it worked.
--
-- What must NOT start working is as much of the point. A type name that is a col_name_keyword --
-- numeric, varchar, integer, boolean, char, bit, time, timestamp, interval -- is read by
-- PostgreSQL's grammar as a type before the parenthesis is reached, so numeric(42) is a syntax
-- error there and not a call at all; none of those names resolves here either. Nor does a call
-- whose argument PostgreSQL has no conversion from: date(42), uuid(42) and int4(point '(1,2)')
-- are each a function that does not exist, and the last of them names the argument "point".
--
-- Alongside it, the geometric constructors. Every one of them answered in text -- point(1,2) was
-- a string, not a point -- and point(), box(), circle(), lseg(), line(), path() and polygon()
-- read the first of an empty argument list and reached the client as XX000 "Internal error:
-- Index: 0". point(42) silently answered (NaN,NaN) where PostgreSQL has no such signature.
--
-- Values that print differently on the two servers are compared through ::text, and the ones
-- that depend on the server's TimeZone or lc_monetary are left out.

-- setup
DROP TABLE IF EXISTS fes_t CASCADE;
DROP DOMAIN IF EXISTS fes_dom CASCADE;
DROP TYPE IF EXISTS fes_enum CASCADE;

CREATE DOMAIN fes_dom AS integer;
CREATE TYPE fes_enum AS ENUM ('a','b');
CREATE TABLE fes_t (i integer, t text, d date, p point, n numeric);
INSERT INTO fes_t VALUES (7, '42', DATE '2020-01-02', point '(1,2)', 3.7);

-- ---------------------------------------------------------------------------
-- 1. The numeric type names, called
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: int4
-- row: 42
-- end-expected
SELECT int4(42);

-- begin-expected
-- columns: int4
-- row: 43
-- end-expected
SELECT int4(42.6);

-- begin-expected
-- columns: int4
-- row: 42
-- end-expected
SELECT int4('42');

-- begin-expected
-- columns: int4
-- row: NULL
-- end-expected
SELECT int4(NULL);

-- begin-expected
-- columns: int2 | int8 | float4 | float8
-- row: 42|42|1.5|1.5
-- end-expected
SELECT int2(42) AS int2, int8(42) AS int8, float4(1.5) AS float4, float8(1.5) AS float8;

-- begin-expected
-- columns: oid | bool
-- row: 42|t
-- end-expected
SELECT oid('42') AS oid, bool('t') AS bool;

-- begin-expected
-- columns: bpchar | name
-- row: ab|ab
-- end-expected
SELECT bpchar('ab') AS bpchar, name('ab') AS name;

-- ---------------------------------------------------------------------------
-- 2. The other built-in type names
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: date
-- row: 2020-01-02
-- end-expected
SELECT date(TIMESTAMP '2020-01-02 03:04:05') AS date;

-- begin-expected
-- columns: inet | cidr | macaddr
-- row: 1.2.3.4|1.2.3.0/24|08:00:2b:01:02:03
-- end-expected
SELECT inet('1.2.3.4') AS inet, cidr('1.2.3.0/24') AS cidr,
       macaddr('08:00:2b:01:02:03') AS macaddr;

-- begin-expected
-- columns: uuid
-- row: 11111111-1111-1111-1111-111111111111
-- end-expected
SELECT uuid('11111111-1111-1111-1111-111111111111') AS uuid;

-- begin-expected
-- columns: jsonb | tsvector | varbit
-- row: {"a": 1}|'a' 'b'|101
-- end-expected
SELECT jsonb('{"a":1}')::text AS jsonb, tsvector('a b')::text AS tsvector,
       varbit('101')::text AS varbit;

-- begin-expected
-- columns: xid | pg_lsn
-- row: 100|0/16B3748
-- end-expected
SELECT xid('100')::text AS xid, pg_lsn('0/16B3748')::text AS pg_lsn;

-- begin-expected
-- columns: int2vector | oidvector
-- row: 1 2|1 2
-- end-expected
SELECT int2vector('1 2')::text AS int2vector, oidvector('1 2')::text AS oidvector;

-- begin-expected
-- columns: bytea
-- row: 616263
-- end-expected
SELECT encode(bytea('abc'), 'hex') AS bytea;

-- begin-expected
-- columns: xml
-- row: <a/>
-- end-expected
SELECT xml('<a/>')::text AS xml;

-- void carries no value: whatever it is handed, PostgreSQL prints nothing at all.
-- begin-expected
-- columns: void
-- row:
-- end-expected
SELECT void('x') AS void;

-- ---------------------------------------------------------------------------
-- 3. The type each of these calls answers in
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: a | b | c | d
-- row: integer|boolean|date|jsonb
-- end-expected
SELECT pg_typeof(int4('42'))::text AS a, pg_typeof(bool('t'))::text AS b,
       pg_typeof(date(TIMESTAMP '2020-01-02'))::text AS c,
       pg_typeof(jsonb('{}'))::text AS d;

-- begin-expected
-- columns: a | b | c
-- row: uuid|inet|tsvector
-- end-expected
SELECT pg_typeof(uuid('11111111-1111-1111-1111-111111111111'))::text AS a,
       pg_typeof(inet('1.2.3.4'))::text AS b,
       pg_typeof(tsvector('a b'))::text AS c;

-- ---------------------------------------------------------------------------
-- 4. A qualifier, and the typmod-applying rows only the qualified spelling reaches
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: int4 | date | bool
-- row: 2|2020-01-02|t
-- end-expected
SELECT pg_catalog.int4(1.9) AS int4, pg_catalog.date('2020-01-02') AS date,
       pg_catalog.bool('t') AS bool;

-- The modifier is the packed one the catalogs hold, so anything below the four-byte header
-- means "no modifier at all" and the value comes back untouched.
-- begin-expected
-- columns: numeric
-- row: 1.5
-- end-expected
SELECT pg_catalog.numeric(1.5, 2) AS numeric;

-- begin-expected
-- columns: varchar
-- row: abcdef
-- end-expected
SELECT pg_catalog.varchar('abcdef'::varchar, 3, true) AS varchar;

-- begin-expected
-- columns: varchar
-- row: abc
-- end-expected
SELECT pg_catalog.varchar('abcdef'::varchar, 7, true) AS varchar;

-- A qualifier that names no schema is the missing schema, not a missing function.
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema_fes" does not exist
-- end-expected-error
SELECT nosuchschema_fes.int4(1.9);

-- ---------------------------------------------------------------------------
-- 5. A domain and an enum the user declared
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: fes_dom
-- row: 1
-- end-expected
SELECT fes_dom(1) AS fes_dom;

-- begin-expected
-- columns: fes_dom
-- row: 1
-- end-expected
SELECT public.fes_dom('1') AS fes_dom;

-- begin-expected
-- columns: fes_enum
-- row: a
-- end-expected
SELECT fes_enum('a') AS fes_enum;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum fes_enum: "zzz"
-- end-expected-error
SELECT fes_enum('zzz');

-- An enum is reached from a string and from nothing else.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function fes_enum(integer) does not exist
-- end-expected-error
SELECT fes_enum(1);

-- ---------------------------------------------------------------------------
-- 6. Where the coercion does not exist, and where the name is not a call at all
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function date(integer) does not exist
-- end-expected-error
SELECT date(42);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function uuid(integer) does not exist
-- end-expected-error
SELECT uuid(42);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function inet(integer) does not exist
-- end-expected-error
SELECT inet(42);

-- The argument is named by the type it was written as, not by the text its value looks like.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4(point) does not exist
-- end-expected-error
SELECT int4(point '(1,2)');

-- A coercion takes one argument. Two is a call, and there is no such call.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4(integer, integer) does not exist
-- end-expected-error
SELECT int4(1, 2);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4() does not exist
-- end-expected-error
SELECT int4();

-- A polymorphic pseudo-type stands for a type rather than being one.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function anyelement(integer) does not exist
-- end-expected-error
SELECT anyelement(1);

-- A schema that exists but holds no such type.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.int4(numeric) does not exist
-- end-expected-error
SELECT public.int4(1.9);

-- ---------------------------------------------------------------------------
-- 7. Columns, and the contexts a call can be written in
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: a | b | c | d
-- row: 42|7|2020-01-02|4
-- end-expected
SELECT int4(t) AS a, int8(i) AS b, date(d) AS c, int4(n) AS d FROM fes_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4(point) does not exist
-- end-expected-error
SELECT int4(p) FROM fes_t;

-- begin-expected
-- columns: a
-- row: 42
-- end-expected
SELECT a FROM (SELECT int4('42') AS a) s;

-- begin-expected
-- columns: a
-- row: 42
-- end-expected
WITH c AS (SELECT int4('42') AS a) SELECT a FROM c;

-- begin-expected
-- columns: int4
-- row: 42
-- end-expected
SELECT (SELECT int4('42')) AS int4;

-- begin-expected
-- columns: i | t
-- row: 7|42
-- end-expected
SELECT i, t FROM fes_t WHERE int4(t) = 42;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM fes_t GROUP BY int4(t);

-- begin-expected
-- columns: bool
-- row: t
-- end-expected
SELECT (jsonb('{"a":1}') -> 'a') = '1'::jsonb AS bool;

-- begin-expected
-- columns: bool
-- row: t
-- end-expected
SELECT tsvector('a b') @@ tsquery('a') AS bool;

CREATE VIEW fes_v AS
    SELECT int4('42') AS a, bool('t') AS b, date('2020-01-02') AS c;

-- begin-expected
-- columns: a | b | c
-- row: 42|t|2020-01-02
-- end-expected
SELECT a, b, c FROM fes_v;

-- ---------------------------------------------------------------------------
-- 8. The geometric constructors: the shape they build, and the calls that have none
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: a | b | c
-- row: point|point|box
-- end-expected
SELECT pg_typeof(point(1,2))::text AS a, pg_typeof(point('(1,2)'))::text AS b,
       pg_typeof(box('(1,1),(0,0)'))::text AS c;

-- begin-expected
-- columns: a | b | c | d
-- row: circle|lseg|path|polygon
-- end-expected
SELECT pg_typeof(circle('<(0,0),5>'))::text AS a,
       pg_typeof(lseg('[(0,0),(1,1)]'))::text AS b,
       pg_typeof(path('[(0,0),(1,1)]'))::text AS c,
       pg_typeof(polygon('((0,0),(1,1),(2,0))'))::text AS d;

-- begin-expected
-- columns: line
-- row: line
-- end-expected
SELECT pg_typeof(line('{1,2,3}'))::text AS line;

-- begin-expected
-- columns: a | b
-- row: (1,2)|(1,1),(0,0)
-- end-expected
SELECT point(1,2)::text AS a, box(point '(0,0)', point '(1,1)')::text AS b;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function point() does not exist
-- end-expected-error
SELECT point();

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function circle() does not exist
-- end-expected-error
SELECT circle();

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function polygon() does not exist
-- end-expected-error
SELECT polygon();

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function point(integer) does not exist
-- end-expected-error
SELECT point(42);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function circle(integer) does not exist
-- end-expected-error
SELECT circle(42);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function path(integer) does not exist
-- end-expected-error
SELECT path(42);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type point: "abc"
-- end-expected-error
SELECT point('abc');

-- cleanup
DROP VIEW IF EXISTS fes_v CASCADE;
DROP TABLE IF EXISTS fes_t CASCADE;
DROP DOMAIN IF EXISTS fes_dom CASCADE;
DROP TYPE IF EXISTS fes_enum CASCADE;
