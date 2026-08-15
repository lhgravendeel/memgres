-- A call carrying a schema is resolved in that schema, and nowhere else.
--
-- public.nosuchfunc(a) is 42883 "function public.nosuchfunc(integer) does not exist" -- the name
-- printed the way it was written, minus its quotes -- while a schema nothing answers to is 3F000
-- and comes first. What the qualifier rules out matters as much: pg_catalog.int8(1) is the
-- built-in coercion, public.int8(1) is a function public does not hold, and the same is true of
-- lower, abs, count and date.
--
-- memgres used to hand a name back unjudged the moment it carried a qualifier other than
-- pg_catalog, so a CHECK naming a function of nobody's defined a constraint and the same call
-- answered at run time. A routine the user declared without naming a schema still belongs to
-- public, so public.<own function> has to keep working in all its spellings.

-- setup
DROP TABLE IF EXISTS zzt4c_qt CASCADE;
DROP SCHEMA IF EXISTS zzt4c_qs CASCADE;
DROP DOMAIN IF EXISTS zzt4c_qdom CASCADE;
DROP FUNCTION IF EXISTS zzt4c_qfn(integer);

CREATE SCHEMA zzt4c_qs;
CREATE SCHEMA "zzt4c_QMix";
CREATE DOMAIN zzt4c_qdom AS integer;
CREATE FUNCTION zzt4c_qfn(integer) RETURNS integer AS 'SELECT $1' LANGUAGE sql IMMUTABLE;
CREATE FUNCTION zzt4c_qs.zzt4c_qsfn(integer) RETURNS integer AS 'SELECT $1' LANGUAGE sql IMMUTABLE;

-- ---------------------------------------------------------------------------
-- 1. A qualified name nothing answers to, in a definition
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.nosuchfunc(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (public.nosuchfunc(a) > 0));

-- The quotes are the writer's, not the message's.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzt4c_QMix.nosuchfunc(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK ("zzt4c_QMix".nosuchfunc(a) > 0));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzt4c_qs.nosuchfunc(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (zzt4c_qs.nosuchfunc(a) > 0));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function information_schema.nosuchfunc(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (information_schema.nosuchfunc(a) > 0));

-- A schema nothing answers to is reported before the function is looked for at all.
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzt4c_nosuchschema" does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (zzt4c_nosuchschema.nosuchfunc(a) > 0));

-- ---------------------------------------------------------------------------
-- 2. The built-ins belong to pg_catalog, so public does not hold them
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.lower(text) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (public.lower(a::text) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.date(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (public.date(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.int8(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (public.int8(a) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.abs(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (public.abs(a) > 0));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.count(integer) does not exist
-- end-expected-error
CREATE TABLE zzt4c_qt (a int, CHECK (public.count(a) > 0));

-- Nothing above got as far as a relation.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzt4c_qt';

-- ---------------------------------------------------------------------------
-- 3. The qualified names that do answer still define their constraints
-- ---------------------------------------------------------------------------

CREATE TABLE zzt4c_qk1 (a int, CHECK (pg_catalog.int8(a) IS NOT NULL));
CREATE TABLE zzt4c_qk2 (a int, CHECK (pg_catalog.abs(a) > 0));
CREATE TABLE zzt4c_qk3 (a int, CHECK (zzt4c_qs.zzt4c_qsfn(a) > 0));
CREATE TABLE zzt4c_qk4 (a int, CHECK (public.zzt4c_qfn(a) > 0));
CREATE TABLE zzt4c_qk5 (a int, CHECK (PUBLIC.zzt4c_qfn(a) > 0));
CREATE TABLE zzt4c_qk6 (a int, CHECK ("public".zzt4c_qfn(a) > 0));
CREATE TABLE zzt4c_qk7 (a int, CHECK (public.zzt4c_qdom(a) IS NOT NULL));

-- begin-expected
-- columns: n
-- row: 7
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzt4c_qk%';

-- ---------------------------------------------------------------------------
-- 4. The same answers where the call is written to run
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.nosuchfunc(integer) does not exist
-- end-expected-error
SELECT public.nosuchfunc(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzt4c_QMix.nosuchfunc(integer) does not exist
-- end-expected-error
SELECT "zzt4c_QMix".nosuchfunc(1);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzt4c_nosuchschema" does not exist
-- end-expected-error
SELECT zzt4c_nosuchschema.nosuchfunc(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.date(integer) does not exist
-- end-expected-error
SELECT public.date(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.int8(integer) does not exist
-- end-expected-error
SELECT public.int8(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.abs(integer) does not exist
-- end-expected-error
SELECT public.abs(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function public.lower(text) does not exist
-- end-expected-error
SELECT public.lower('A'::text);

-- begin-expected
-- columns: a | b
-- row: 1|2
-- end-expected
SELECT pg_catalog.int8(1) AS a, pg_catalog.abs(-2) AS b;

-- begin-expected
-- columns: a | b | c
-- row: 1|1|1
-- end-expected
SELECT zzt4c_qs.zzt4c_qsfn(1) AS a, public.zzt4c_qfn(1) AS b, public.zzt4c_qdom(1) AS c;

-- cleanup
DROP TABLE IF EXISTS zzt4c_qk1, zzt4c_qk2, zzt4c_qk3, zzt4c_qk4, zzt4c_qk5, zzt4c_qk6, zzt4c_qk7 CASCADE;
DROP FUNCTION IF EXISTS zzt4c_qs.zzt4c_qsfn(integer);
DROP FUNCTION IF EXISTS zzt4c_qfn(integer);
DROP DOMAIN IF EXISTS zzt4c_qdom CASCADE;
DROP SCHEMA IF EXISTS zzt4c_qs CASCADE;
DROP SCHEMA IF EXISTS "zzt4c_QMix" CASCADE;
