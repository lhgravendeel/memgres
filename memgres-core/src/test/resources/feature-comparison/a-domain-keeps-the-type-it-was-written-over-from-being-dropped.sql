-- ============================================================================
-- A domain depends on the type it was written over
--
-- CREATE DOMAIN d2 AS d1 writes d2 in terms of d1, so d1 cannot be dropped
-- while d2 stands: the drop is refused with 2BP01 whether it was written as
-- DROP DOMAIN or as DROP TYPE, and both types are still there afterwards. It
-- holds for a domain written over a domain, over a composite, over an enum,
-- and over an array of a domain.
--
-- CASCADE takes the dependent domain, and everything written in terms of that
-- domain in turn: a column declared with it, and a domain written over it,
-- and a column declared with that one. The relations themselves stand, having
-- lost the columns whose type went. A schema is refused and cascaded the same
-- way, reaching the domains and columns outside it that were written over a
-- type inside it. Every answer here was read off PostgreSQL 18.
-- ============================================================================

-- setup
CREATE DOMAIN zzt4d_ud1 AS int;
CREATE DOMAIN zzt4d_ud2 AS zzt4d_ud1;

-- ----------------------------------------------------------------------------
-- The base cannot be dropped from under the domain
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_ud1 because other objects depend on it
-- end-expected-error
DROP DOMAIN zzt4d_ud1;

-- a domain is a type, and the drop reads the same written either way
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_ud1 because other objects depend on it
-- end-expected-error
DROP TYPE zzt4d_ud1;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_ud1 because other objects depend on it
-- end-expected-error
DROP DOMAIN zzt4d_ud1 RESTRICT;

-- and both are still there
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT 1::zzt4d_ud2::text AS v;

DROP DOMAIN zzt4d_ud1 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_type WHERE typname IN ('zzt4d_ud1','zzt4d_ud2');

-- ----------------------------------------------------------------------------
-- CASCADE reaches everything written in terms of what it takes
-- ----------------------------------------------------------------------------
CREATE DOMAIN zzt4d_ud1 AS int;
CREATE DOMAIN zzt4d_ud2 AS zzt4d_ud1;
CREATE TABLE zzt4d_uta (c zzt4d_ud2, k int);
CREATE DOMAIN zzt4d_ud3 AS zzt4d_ud2;
CREATE TABLE zzt4d_utb (c zzt4d_ud3, k int);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_ud1 because other objects depend on it
-- end-expected-error
DROP DOMAIN zzt4d_ud1;

DROP DOMAIN zzt4d_ud1 CASCADE;

-- every domain in the chain went
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_type WHERE typname IN ('zzt4d_ud1','zzt4d_ud2','zzt4d_ud3');

-- the relations stand, each having lost the column whose type went
-- begin-expected
-- columns: t | cols
-- row: zzt4d_uta | k
-- row: zzt4d_utb | k
-- end-expected
SELECT table_name AS t, string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name IN ('zzt4d_uta','zzt4d_utb') GROUP BY table_name ORDER BY table_name;

-- ----------------------------------------------------------------------------
-- A domain over a composite, over an enum and over an array reads the same
-- ----------------------------------------------------------------------------
CREATE TYPE zzt4d_uc AS (x int);
CREATE DOMAIN zzt4d_udc AS zzt4d_uc;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_uc because other objects depend on it
-- end-expected-error
DROP TYPE zzt4d_uc;

CREATE TYPE zzt4d_ue AS ENUM ('a','b');
CREATE DOMAIN zzt4d_ude AS zzt4d_ue;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_ue because other objects depend on it
-- end-expected-error
DROP TYPE zzt4d_ue;

CREATE DOMAIN zzt4d_ub1 AS int;
CREATE DOMAIN zzt4d_ub2 AS zzt4d_ub1[];

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_ub1 because other objects depend on it
-- end-expected-error
DROP DOMAIN zzt4d_ub1;

DROP TYPE zzt4d_uc CASCADE;
DROP TYPE zzt4d_ue CASCADE;
DROP DOMAIN zzt4d_ub1 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_type WHERE typname IN ('zzt4d_udc','zzt4d_ude','zzt4d_ub2');

-- ----------------------------------------------------------------------------
-- A schema is refused for the same reason, and reaches outside itself
-- ----------------------------------------------------------------------------
CREATE SCHEMA zzt4d_us;
CREATE TYPE zzt4d_us.zzt4d_use AS ENUM ('a','b');
CREATE DOMAIN public.zzt4d_usd AS zzt4d_us.zzt4d_use;
CREATE TABLE public.zzt4d_ust (c zzt4d_us.zzt4d_use, k int);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema zzt4d_us because other objects depend on it
-- end-expected-error
DROP SCHEMA zzt4d_us;

DROP SCHEMA zzt4d_us CASCADE;

-- the domain outside the schema went with the type inside it
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_type WHERE typname = 'zzt4d_usd';

-- and so did the column, while the relation stands
-- begin-expected
-- columns: cols
-- row: k
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzt4d_ust';

-- cleanup
DROP TABLE zzt4d_uta;
DROP TABLE zzt4d_utb;
DROP TABLE zzt4d_ust;
