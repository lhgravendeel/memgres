-- A composite type is a type: it lives in a schema the search path either reaches or does
-- not, its attributes carry the modifiers and the storage layout of the types they were
-- declared with, a column declared with it points at it rather than at text, a value
-- written into such a column is coerced to it field by field, an attribute change is
-- refused while a relation depends on the type, and every attribute change a transaction
-- makes is undone when the transaction rolls back.
-- Every answer below was read off PostgreSQL 18.

-- stmt 1: a type outside the search path is not reachable by its bare name
CREATE SCHEMA cti_hidden;
CREATE DOMAIN cti_hidden.cti_d AS int;
CREATE TYPE cti_hidden.cti_ct AS (x int);
CREATE TYPE cti_hidden.cti_rg AS RANGE (subtype = int4);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_d" does not exist
-- end-expected-error
SELECT 1::cti_d;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_ct" does not exist
-- end-expected-error
SELECT row(1)::cti_ct;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_rg" does not exist
-- end-expected-error
SELECT '[1,3)'::cti_rg;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_d" does not exist
-- end-expected-error
ALTER DOMAIN cti_d SET NOT NULL;

-- the qualified name still reaches it
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT 1::cti_hidden.cti_d AS v;

-- stmt 2: putting the schema on the search path makes the bare name reach it
SET search_path TO cti_hidden, public;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT 1::cti_d AS v;

-- begin-expected
-- columns: v
-- row: (1)
-- end-expected
SELECT (row(1)::cti_ct)::text AS v;

-- begin-expected
-- columns: v
-- row: [1,3)
-- end-expected
SELECT ('[1,3)'::cti_rg)::text AS v;

-- stmt 3: and RESET takes it away again
RESET search_path;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_d" does not exist
-- end-expected-error
SELECT 1::cti_d;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_ct" does not exist
-- end-expected-error
SELECT row(1)::cti_ct;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_rg" does not exist
-- end-expected-error
SELECT '[1,3)'::cti_rg;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cti_d" does not exist
-- end-expected-error
ALTER DOMAIN cti_d SET NOT NULL;

DROP SCHEMA cti_hidden CASCADE;

-- stmt 4: ROLLBACK undoes every attribute change, tombstone included
CREATE TYPE cti_undo AS (x int, y int);

BEGIN;

ALTER TYPE cti_undo ADD ATTRIBUTE z text;

ALTER TYPE cti_undo RENAME ATTRIBUTE x TO xx;

ALTER TYPE cti_undo ALTER ATTRIBUTE y TYPE varchar(4);

ALTER TYPE cti_undo DROP ATTRIBUTE y;

-- begin-expected
-- columns: attname | attnum | attisdropped | t
-- row: xx | 1 | f | integer
-- row: ........pg.dropped.2........ | 2 | t | -
-- row: z | 3 | f | text
-- end-expected
SELECT a.attname, a.attnum, a.attisdropped, format_type(a.atttypid, a.atttypmod) AS t FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_undo' AND a.attnum > 0 ORDER BY a.attnum;

ROLLBACK;

-- begin-expected
-- columns: attname | attnum | attisdropped | t
-- row: x | 1 | f | integer
-- row: y | 2 | f | integer
-- end-expected
SELECT a.attname, a.attnum, a.attisdropped, format_type(a.atttypid, a.atttypmod) AS t FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_undo' AND a.attnum > 0 ORDER BY a.attnum;

-- stmt 5: ROLLBACK TO a savepoint undoes only what came after it
BEGIN;

ALTER TYPE cti_undo ADD ATTRIBUTE z text;

SAVEPOINT cti_sp;

ALTER TYPE cti_undo ADD ATTRIBUTE w text;

ROLLBACK TO cti_sp;

-- begin-expected
-- columns: attname | attnum
-- row: x | 1
-- row: y | 2
-- row: z | 3
-- end-expected
SELECT a.attname, a.attnum FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_undo' AND a.attnum > 0 ORDER BY a.attnum;

COMMIT;

-- begin-expected
-- columns: attname | attnum
-- row: x | 1
-- row: y | 2
-- row: z | 3
-- end-expected
SELECT a.attname, a.attnum FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_undo' AND a.attnum > 0 ORDER BY a.attnum;

DROP TYPE cti_undo;

-- stmt 6: a column declared with a composite points at the composite
CREATE TYPE cti_pair AS (v varchar(3), n int);
CREATE TABLE cti_holder (c cti_pair);

-- begin-expected
-- columns: t | attlen | attstorage | attalign | same
-- row: cti_pair | -1 | x | d | t
-- end-expected
SELECT format_type(a.atttypid, a.atttypmod) AS t, a.attlen, a.attstorage, a.attalign, (a.atttypid = 'cti_pair'::regtype) AS same FROM pg_attribute a WHERE a.attrelid = 'cti_holder'::regclass AND a.attnum > 0;

-- begin-expected
-- columns: data_type | udt_name
-- row: USER-DEFINED | cti_pair
-- end-expected
SELECT data_type, udt_name FROM information_schema.columns WHERE table_name = 'cti_holder';

CREATE TABLE cti_arr (c cti_pair[]);

-- begin-expected
-- columns: t
-- row: cti_pair[]
-- end-expected
SELECT format_type(a.atttypid, a.atttypmod) AS t FROM pg_attribute a WHERE a.attrelid = 'cti_arr'::regclass AND a.attnum > 0;

-- stmt 7: a write into such a column is held to each field's width and to the arity
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
INSERT INTO cti_holder VALUES (row('abcdef', 3));

-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cti_pair
-- end-expected-error
INSERT INTO cti_holder VALUES (row(1,2,3));

-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cti_pair
-- end-expected-error
INSERT INTO cti_holder VALUES (row(1));

INSERT INTO cti_holder VALUES (row('ab', 3));

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
UPDATE cti_holder SET c = row('abcdef', 3);

-- the refused update left the stored row alone
-- begin-expected
-- columns: v
-- row: (ab,3)
-- end-expected
SELECT c::text AS v FROM cti_holder;

DROP TABLE cti_arr;
DROP TABLE cti_holder;
DROP TYPE cti_pair;

-- stmt 8: a field is padded and rounded as its own type declares
CREATE TYPE cti_pad AS (a char(3), b numeric(4,1));
CREATE TABLE cti_padt (c cti_pad);
INSERT INTO cti_padt VALUES (row('a', 1.26));

-- begin-expected
-- columns: v | b
-- row: ("a  ",1.3) | 1.3
-- end-expected
SELECT c::text AS v, (c).b AS b FROM cti_padt;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
INSERT INTO cti_padt VALUES (row('a', 12345.6));

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character(3)
-- end-expected-error
INSERT INTO cti_padt VALUES (row('abcd', 1));

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM cti_padt;

DROP TABLE cti_padt;
DROP TYPE cti_pad;

-- stmt 9: an attribute keeps the interval field qualifier it was declared with
CREATE TYPE cti_iv AS (a interval hour to minute, b interval day, c interval(3), d interval second(2), e interval, f interval day to second(4), g interval[]);

-- begin-expected
-- columns: attname | t | atttypmod
-- row: a | interval hour to minute | 201392127
-- row: b | interval day | 589823
-- row: c | interval(3) | 2147418115
-- row: d | interval second(2) | 268435458
-- row: e | interval | -1
-- row: f | interval day to second(4) | 470286340
-- row: g | interval[] | -1
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS t, a.atttypmod FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_iv' AND a.attnum > 0 ORDER BY a.attnum;

DROP TYPE cti_iv;

-- stmt 10: an attribute reports the storage layout of its declared type, not a fixed one
CREATE TYPE cti_lay AS (a int, b varchar(5), c text, d numeric(10,2), e bool, f timestamp(3), g uuid, h bytea, i char(3), j float8, k smallint, l date, m point, n int[], o varchar(5)[], p bigint, q jsonb, s name, t oid);

-- begin-expected
-- columns: attname | attlen | attstorage | attalign | attbyval
-- row: a | 4 | p | i | t
-- row: b | -1 | x | i | f
-- row: c | -1 | x | i | f
-- row: d | -1 | m | i | f
-- row: e | 1 | p | c | t
-- row: f | 8 | p | d | t
-- row: g | 16 | p | c | f
-- row: h | -1 | x | i | f
-- row: i | -1 | x | i | f
-- row: j | 8 | p | d | t
-- row: k | 2 | p | s | t
-- row: l | 4 | p | i | t
-- row: m | 16 | p | d | f
-- row: n | -1 | x | i | f
-- row: o | -1 | x | i | f
-- row: p | 8 | p | d | t
-- row: q | -1 | x | i | f
-- row: s | 64 | p | c | f
-- row: t | 4 | p | i | t
-- end-expected
SELECT a.attname, a.attlen, a.attstorage, a.attalign, a.attbyval FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_lay' AND a.attnum > 0 ORDER BY a.attnum;

DROP TYPE cti_lay;

-- an attribute of a user-declared type reports that type's own layout
CREATE TYPE cti_e AS ENUM ('a','b');
CREATE DOMAIN cti_d2 AS varchar(4);
CREATE TYPE cti_in AS (q int);
CREATE TYPE cti_u AS (a cti_e, b cti_d2, c cti_in, d cti_e[], e cti_in[]);

-- begin-expected
-- columns: attname | t | attlen | attstorage | attalign | attbyval
-- row: a | cti_e | 4 | p | i | t
-- row: b | cti_d2 | -1 | x | i | f
-- row: c | cti_in | -1 | x | d | f
-- row: d | cti_e[] | -1 | x | i | f
-- row: e | cti_in[] | -1 | x | d | f
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS t, a.attlen, a.attstorage, a.attalign, a.attbyval FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_u' AND a.attnum > 0 ORDER BY a.attnum;

DROP TYPE cti_u;
DROP TYPE cti_in;
DROP DOMAIN cti_d2;
DROP TYPE cti_e;

-- stmt 11: a dropped attribute keeps the layout it was declared with, and its number
CREATE TYPE cti_dr AS (a int, b timestamp(3), c point, d int[]);
ALTER TYPE cti_dr DROP ATTRIBUTE b;
ALTER TYPE cti_dr DROP ATTRIBUTE c;
ALTER TYPE cti_dr DROP ATTRIBUTE d;

-- begin-expected
-- columns: attname | attnum | attisdropped | atttypid | atttypmod | attlen | attstorage | attalign | attbyval
-- row: a | 1 | f | 23 | -1 | 4 | p | i | t
-- row: ........pg.dropped.2........ | 2 | t | 0 | 3 | 8 | p | d | t
-- row: ........pg.dropped.3........ | 3 | t | 0 | -1 | 16 | p | d | f
-- row: ........pg.dropped.4........ | 4 | t | 0 | -1 | -1 | x | i | f
-- end-expected
SELECT a.attname, a.attnum, a.attisdropped, a.atttypid, a.atttypmod, a.attlen, a.attstorage, a.attalign, a.attbyval FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid WHERE t.typname = 'cti_dr' AND a.attnum > 0 ORDER BY a.attnum;

DROP TYPE cti_dr;

-- stmt 12: an attribute's type cannot be changed while a column uses the type
CREATE TYPE cti_used AS (x int);
CREATE TABLE cti_uses (c cti_used);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type "cti_used" because column "cti_uses.c" uses it
-- end-expected-error
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text;

-- CASCADE does not excuse a plain column
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type "cti_used" because column "cti_uses.c" uses it
-- end-expected-error
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text CASCADE;

-- the other three attribute actions are allowed
ALTER TYPE cti_used ADD ATTRIBUTE y text;
ALTER TYPE cti_used RENAME ATTRIBUTE y TO z;
ALTER TYPE cti_used DROP ATTRIBUTE z;

DROP TABLE cti_uses;
DROP TYPE cti_used;

-- stmt 13: the column reaches the type through an array, a composite or a domain
CREATE TYPE cti_used AS (x int);
CREATE TABLE cti_v1 (c cti_used[]);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type "cti_used" because column "cti_v1.c" uses it
-- end-expected-error
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text;

DROP TABLE cti_v1;
CREATE TYPE cti_v2t AS (q cti_used);
CREATE TABLE cti_v2 (c cti_v2t);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type "cti_used" because column "cti_v2.c" uses it
-- end-expected-error
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text;

DROP TABLE cti_v2;
DROP TYPE cti_v2t;
CREATE DOMAIN cti_v3d AS cti_used;
CREATE TABLE cti_v3 (c cti_v3d);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type "cti_used" because column "cti_v3.c" uses it
-- end-expected-error
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text;

DROP TABLE cti_v3;
DROP DOMAIN cti_v3d;

-- the relation is named bare even when it lives in a schema the search path does not reach
CREATE SCHEMA cti_sc2;
CREATE TABLE cti_sc2.cti_tb (m cti_used);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type "cti_used" because column "cti_tb.m" uses it
-- end-expected-error
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text;

DROP SCHEMA cti_sc2 CASCADE;

-- a view over the type does not block the change
CREATE VIEW cti_vw AS SELECT c FROM (SELECT NULL::cti_used AS c) s;
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text;
DROP VIEW cti_vw;
DROP TYPE cti_used;

-- stmt 14: a typed table is refused with 2BP01 and a hint of its own
CREATE TYPE cti_used AS (x int);
CREATE TABLE cti_typed OF cti_used;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot alter type "cti_used" because it is the type of a typed table
-- end-expected-error
ALTER TYPE cti_used ALTER ATTRIBUTE x TYPE text;

-- begin-expected
-- columns: attname | t
-- row: x | integer
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS t FROM pg_attribute a WHERE a.attrelid = 'cti_typed'::regclass AND a.attnum > 0;

-- cleanup
DROP TABLE cti_typed;
DROP TYPE cti_used;
