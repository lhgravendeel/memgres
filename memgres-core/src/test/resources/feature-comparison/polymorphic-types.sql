-- ============================================================================
-- Feature Comparison: polymorphic pseudo-types and the per-schema function namespace
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers two related gaps in how routines are named and typed:
--
--   1. anyelement / anyarray / anynonarray / anycompatible (and the rest of the
--      polymorphic family) exist as pg_type rows, can be declared in a routine
--      signature, resolve from the actual argument types at each call, and make
--      the result type follow. Signatures PostgreSQL rejects -- a polymorphic
--      result with no polymorphic argument to determine it from -- give 42P13,
--      and a call whose arguments cannot bind the slots gives 42883.
--
--   2. Functions live in a per-schema namespace, so two schemas may each define
--      a function of the same name, search_path decides which an unqualified
--      call reaches, and DROP only removes the copy it names.
--
-- Every object created here is prefixed ptf_.
-- ============================================================================

DROP SCHEMA IF EXISTS ptf_s1 CASCADE;
DROP SCHEMA IF EXISTS ptf_s2 CASCADE;
DROP FUNCTION IF EXISTS ptf_ident(anyelement);
DROP FUNCTION IF EXISTS ptf_first(anyarray);
DROP FUNCTION IF EXISTS ptf_wrap(anyelement);
DROP FUNCTION IF EXISTS ptf_nonarr(anynonarray);
DROP FUNCTION IF EXISTS ptf_cmp(anycompatible, anycompatible);
DROP FUNCTION IF EXISTS ptf_pair(anyelement, anyelement);
DROP FUNCTION IF EXISTS ptf_plp(anyelement);
DROP FUNCTION IF EXISTS ptf_plain(int);
DROP FUNCTION IF EXISTS ptf_plain(text);
DROP TABLE IF EXISTS ptf_t;

-- ============================================================================
-- 1. The polymorphic pseudo-types are registered
-- ============================================================================

SELECT typname, typtype, typcategory, typlen, typalign, typstorage
FROM pg_type WHERE typname LIKE 'any%' ORDER BY typname;

SELECT oid FROM pg_type WHERE typname = 'anyelement';
SELECT oid FROM pg_type WHERE typname = 'anyarray';
SELECT oid FROM pg_type WHERE typname = 'anynonarray';
SELECT oid FROM pg_type WHERE typname = 'anycompatible';
SELECT oid FROM pg_type WHERE typname = 'anycompatiblemultirange';

SELECT 'anyelement'::regtype AS t;
SELECT 'anyarray'::regtype AS t;
SELECT 'anynonarray'::regtype AS t;
SELECT 'anycompatible'::regtype AS t;
SELECT 2283::regtype AS t;
SELECT 2277::regtype AS t;
SELECT 2776::regtype AS t;
SELECT 5077::regtype AS t;

-- ============================================================================
-- 2. anyelement: the result type follows the argument
-- ============================================================================

CREATE FUNCTION ptf_ident(x anyelement) RETURNS anyelement LANGUAGE sql AS $$ SELECT x $$;

SELECT ptf_ident(5);
SELECT pg_typeof(ptf_ident(5))::text AS t;
SELECT ptf_ident('abc'::text);
SELECT pg_typeof(ptf_ident('abc'::text))::text AS t;
SELECT pg_typeof(ptf_ident(1.5))::text AS t;
SELECT pg_typeof(ptf_ident(true))::text AS t;

-- A NULL that carries a type still determines the polymorph; a bare NULL does not.
SELECT ptf_ident(NULL::int);
SELECT pg_typeof(ptf_ident(NULL::int))::text AS t;
SELECT ptf_ident(NULL);

-- ============================================================================
-- 3. anyarray and anynonarray constrain the shape of the argument
-- ============================================================================

CREATE FUNCTION ptf_first(a anyarray) RETURNS anyelement LANGUAGE sql AS $$ SELECT a[1] $$;

SELECT ptf_first(ARRAY[10, 20, 30]);
SELECT pg_typeof(ptf_first(ARRAY[10, 20, 30]))::text AS t;
SELECT ptf_first(ARRAY['a', 'b']::text[]);
SELECT pg_typeof(ptf_first(ARRAY['a', 'b']::text[]))::text AS t;
SELECT ptf_first(5);

CREATE FUNCTION ptf_wrap(x anyelement) RETURNS anyarray LANGUAGE sql AS $$ SELECT ARRAY[x] $$;

SELECT ptf_wrap(7);
SELECT pg_typeof(ptf_wrap(7))::text AS t;
SELECT pg_typeof(ptf_wrap('z'::text))::text AS t;

CREATE FUNCTION ptf_nonarr(x anynonarray) RETURNS text LANGUAGE sql AS $$ SELECT x::text $$;

SELECT ptf_nonarr(9);
SELECT ptf_nonarr(NULL::int);
SELECT ptf_nonarr(ARRAY[1, 2]);

-- Two anyelement slots must land on the same type.
CREATE FUNCTION ptf_pair(a anyelement, b anyelement) RETURNS anyelement LANGUAGE sql AS $$ SELECT a $$;

SELECT ptf_pair(1, 2);
SELECT ptf_pair(1, 'x'::text);

-- ============================================================================
-- 4. anycompatible only needs a common type, not an identical one
-- ============================================================================

CREATE FUNCTION ptf_cmp(a anycompatible, b anycompatible) RETURNS anycompatible
LANGUAGE sql AS $$ SELECT CASE WHEN a > b THEN a ELSE b END $$;

SELECT ptf_cmp(2, 5);
SELECT pg_typeof(ptf_cmp(2, 5))::text AS t;
SELECT ptf_cmp(2, 5.5);
SELECT pg_typeof(ptf_cmp(2, 5.5))::text AS t;

-- ============================================================================
-- 5. Signatures PostgreSQL rejects with 42P13
-- ============================================================================

CREATE FUNCTION ptf_noarg() RETURNS anyelement LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION ptf_noarg2() RETURNS anyarray LANGUAGE sql AS $$ SELECT ARRAY[1] $$;
CREATE FUNCTION ptf_noarg3() RETURNS anynonarray LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION ptf_noarg4() RETURNS anycompatible LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION ptf_badarr(x int) RETURNS anyarray LANGUAGE sql AS $$ SELECT ARRAY[x] $$;
CREATE FUNCTION ptf_cross(x anyelement) RETURNS anycompatible LANGUAGE sql AS $$ SELECT x $$;

-- ============================================================================
-- 6. Polymorphism in PL/pgSQL, over columns, and through CREATE OR REPLACE
-- ============================================================================

CREATE FUNCTION ptf_plp(x anyelement) RETURNS anyelement LANGUAGE plpgsql AS $$ BEGIN RETURN x; END $$;

SELECT ptf_plp(3);
SELECT pg_typeof(ptf_plp(3))::text AS t;
SELECT pg_typeof(ptf_plp('q'::text))::text AS t;

CREATE TABLE ptf_t (a int, b text);
INSERT INTO ptf_t VALUES (1, 'x'), (2, 'y');
SELECT ptf_ident(a), ptf_ident(b) FROM ptf_t ORDER BY 1;
SELECT pg_typeof(ptf_ident(a))::text AS t FROM ptf_t LIMIT 1;
DROP TABLE ptf_t;

CREATE OR REPLACE FUNCTION ptf_ident(x anyelement) RETURNS anyelement LANGUAGE sql AS $$ SELECT x $$;
SELECT ptf_ident(6);

SELECT proname, pg_get_function_arguments(oid), pg_get_function_result(oid)
FROM pg_proc WHERE proname = 'ptf_ident';

-- ============================================================================
-- 7. Non-polymorphic routines keep resolving by argument type
-- ============================================================================

CREATE FUNCTION ptf_plain(x int) RETURNS int LANGUAGE sql AS $$ SELECT x + 1 $$;
SELECT ptf_plain(4);
SELECT pg_typeof(ptf_plain(4))::text AS t;
SELECT ptf_plain('a');

CREATE FUNCTION ptf_plain(x text) RETURNS text LANGUAGE sql AS $$ SELECT upper(x) $$;
SELECT ptf_plain('ab'::text);
SELECT ptf_plain(4);
DROP FUNCTION ptf_plain(int);
DROP FUNCTION ptf_plain(text);
SELECT ptf_plain(4);

-- ============================================================================
-- 8. Functions are a per-schema namespace
-- ============================================================================

CREATE SCHEMA ptf_s1;
CREATE SCHEMA ptf_s2;
CREATE FUNCTION public.ptf_pubfn() RETURNS int LANGUAGE sql AS $$ SELECT 99 $$;
CREATE FUNCTION ptf_s1.ptf_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION ptf_s2.ptf_f() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;

SELECT ptf_s1.ptf_f();
SELECT ptf_s2.ptf_f();

SELECT n.nspname, p.proname FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE p.proname = 'ptf_f' ORDER BY 1;

-- search_path decides which one an unqualified call reaches.
SET search_path = ptf_s1, ptf_s2;
SELECT ptf_f();
SET search_path = ptf_s2, ptf_s1;
SELECT ptf_f();
SET search_path = public;
SELECT ptf_f();

-- DROP removes only the copy it names.
DROP FUNCTION ptf_s1.ptf_f();
SELECT ptf_s2.ptf_f();
SELECT ptf_s1.ptf_f();

-- Same name, same schema, same argument types is still a duplicate.
CREATE FUNCTION ptf_s2.ptf_f() RETURNS int LANGUAGE sql AS $$ SELECT 3 $$;
-- A different argument list in the same schema is an overload.
CREATE FUNCTION ptf_s2.ptf_f(x int) RETURNS int LANGUAGE sql AS $$ SELECT x $$;
SELECT ptf_s2.ptf_f(41);

-- Qualified built-ins and public functions still resolve and label as PG does.
SELECT pg_catalog.upper('ab');
SELECT upper('ab');
SELECT length('abc');
SELECT public.ptf_pubfn();

-- An unqualified DROP only reaches what search_path can see.
SET search_path = public;
DROP FUNCTION IF EXISTS ptf_f();
SELECT ptf_s2.ptf_f();

SET search_path = ptf_s2, public;
SELECT current_schema();
SELECT ptf_f();
SET search_path = public;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA ptf_s1 CASCADE;
DROP SCHEMA ptf_s2 CASCADE;
DROP FUNCTION IF EXISTS ptf_ident(anyelement);
DROP FUNCTION IF EXISTS ptf_first(anyarray);
DROP FUNCTION IF EXISTS ptf_wrap(anyelement);
DROP FUNCTION IF EXISTS ptf_nonarr(anynonarray);
DROP FUNCTION IF EXISTS ptf_cmp(anycompatible, anycompatible);
DROP FUNCTION IF EXISTS ptf_pair(anyelement, anyelement);
DROP FUNCTION IF EXISTS ptf_plp(anyelement);
DROP FUNCTION IF EXISTS public.ptf_pubfn();
