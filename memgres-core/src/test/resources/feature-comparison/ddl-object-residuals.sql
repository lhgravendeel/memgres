-- ============================================================================
-- Feature Comparison: DDL on objects that are not tables
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Five groups, all of them about a statement that named something.
--
-- ALTER on a name that was never created reported success for most object kinds
-- that memgres accepts without implementing -- the text search objects, the
-- foreign-data wrappers and servers, subscriptions and extensions -- so a script
-- that renamed the wrong thing learned nothing. DROP on the same names was
-- silent in the same way.
--
-- ALTER SCHEMA ... OWNER TO on a schema that is not there was accepted too.
--
-- A domain default written as a quoted literal is read with the base type's
-- input function when the domain is defined, so one that is not a value of the
-- type is 22P02 there rather than at the first insert -- and what the catalogs
-- then report is the value that was read, not the text it was written as. An
-- attribute, by contrast, lives on the relation a composite type owns, so
-- ALTER TYPE ... ADD ATTRIBUTE on a name that owns no relation is 42P01.
--
-- Which clause a policy may carry follows from the command it guards, and
-- ALTER POLICY cannot change that command, so the clause is checked there too.
--
-- And a table an inheritance child depends on cannot be dropped while the child
-- is there; the Detail and the Hint say what depends on it and what to do.
--
-- The harness keeps only the first line of an error, so the Detail and Hint
-- lines below are written as detail-like:/hint-like: and are documentation of
-- what both engines send in those fields rather than something it compares.
-- Notices are not compared by the harness at all; DdlObjectResidualsTest reads
-- them as SQLWarning instead.
-- ============================================================================

-- ============================================================================
-- 1. ALTER on an object kind that memgres accepts without implementing
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "bo_nosuch" does not exist
-- end-expected-error
ALTER TEXT SEARCH CONFIGURATION bo_nosuch RENAME TO bo_other;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "bo_nosuch" does not exist
-- end-expected-error
ALTER TEXT SEARCH CONFIGURATION bo_nosuch ADD MAPPING FOR word WITH simple;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search dictionary "bo_nosuch" does not exist
-- end-expected-error
ALTER TEXT SEARCH DICTIONARY bo_nosuch RENAME TO bo_other;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search parser "bo_nosuch" does not exist
-- end-expected-error
ALTER TEXT SEARCH PARSER bo_nosuch RENAME TO bo_other;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search template "bo_nosuch" does not exist
-- end-expected-error
ALTER TEXT SEARCH TEMPLATE bo_nosuch RENAME TO bo_other;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: foreign-data wrapper "bo_nosuch" does not exist
-- end-expected-error
ALTER FOREIGN DATA WRAPPER bo_nosuch RENAME TO bo_other;

-- the OPTIONS spelling is as unchecked as the rename was
-- begin-expected-error
-- sqlstate: 42704
-- message-like: foreign-data wrapper "bo_nosuch" does not exist
-- end-expected-error
ALTER FOREIGN DATA WRAPPER bo_nosuch OPTIONS (a 'b');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: server "bo_nosuch" does not exist
-- end-expected-error
ALTER SERVER bo_nosuch RENAME TO bo_other;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: server "bo_nosuch" does not exist
-- end-expected-error
ALTER SERVER bo_nosuch OPTIONS (a 'b');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: server "bo_nosuch" does not exist
-- end-expected-error
ALTER USER MAPPING FOR CURRENT_USER SERVER bo_nosuch OPTIONS (SET a 'b');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: subscription "bo_nosuch" does not exist
-- end-expected-error
ALTER SUBSCRIPTION bo_nosuch RENAME TO bo_other;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: subscription "bo_nosuch" does not exist
-- end-expected-error
ALTER SUBSCRIPTION bo_nosuch ENABLE;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: extension "bo_nosuch" does not exist
-- end-expected-error
ALTER EXTENSION bo_nosuch UPDATE;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: extension "bo_nosuch" does not exist
-- end-expected-error
ALTER EXTENSION bo_nosuch SET SCHEMA public;

-- a foreign table lives in the relation namespace, so it is reported as one
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bo_nosuch" does not exist
-- end-expected-error
ALTER FOREIGN TABLE bo_nosuch RENAME TO bo_other;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: large object 987654 does not exist
-- end-expected-error
ALTER LARGE OBJECT 987654 OWNER TO CURRENT_USER;

-- ============================================================================
-- 2. DROP on the same kinds, and IF EXISTS on them
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "bo_nosuch" does not exist
-- end-expected-error
DROP TEXT SEARCH CONFIGURATION bo_nosuch;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search dictionary "bo_nosuch" does not exist
-- end-expected-error
DROP TEXT SEARCH DICTIONARY bo_nosuch;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: server "bo_nosuch" does not exist
-- end-expected-error
DROP SERVER bo_nosuch;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: foreign-data wrapper "bo_nosuch" does not exist
-- end-expected-error
DROP FOREIGN DATA WRAPPER bo_nosuch;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: publication "bo_nosuch" does not exist
-- end-expected-error
DROP PUBLICATION bo_nosuch;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: extension "bo_nosuch" does not exist
-- end-expected-error
DROP EXTENSION bo_nosuch;

-- IF EXISTS still takes every one of them (PostgreSQL adds a NOTICE naming
-- what it skipped; the harness does not read notices, the unit test does)
DROP TEXT SEARCH CONFIGURATION IF EXISTS bo_nosuch;
DROP TEXT SEARCH DICTIONARY IF EXISTS bo_nosuch;
DROP TEXT SEARCH PARSER IF EXISTS bo_nosuch;
DROP TEXT SEARCH TEMPLATE IF EXISTS bo_nosuch;
DROP SERVER IF EXISTS bo_nosuch;
DROP FOREIGN DATA WRAPPER IF EXISTS bo_nosuch;
DROP PUBLICATION IF EXISTS bo_nosuch;
DROP EXTENSION IF EXISTS bo_nosuch;
DROP VIEW IF EXISTS bo_nosuch;
DROP SEQUENCE IF EXISTS bo_nosuch;
DROP INDEX IF EXISTS bo_nosuch;
DROP TYPE IF EXISTS bo_nosuch;
DROP DOMAIN IF EXISTS bo_nosuch;
DROP FUNCTION IF EXISTS bo_nosuch(int, text);

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT 't'::text AS ok;

-- ============================================================================
-- 3. The same ALTERs on objects that are there are still taken
-- ============================================================================
DROP TEXT SEARCH CONFIGURATION IF EXISTS bo_cfg;
DROP TEXT SEARCH CONFIGURATION IF EXISTS bo_cfg2;
CREATE TEXT SEARCH CONFIGURATION bo_cfg (COPY = simple);
ALTER TEXT SEARCH CONFIGURATION bo_cfg RENAME TO bo_cfg2;

-- begin-expected
-- columns: gone, there
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_ts_config WHERE cfgname = 'bo_cfg')::text AS gone,
       (SELECT count(*) FROM pg_ts_config WHERE cfgname = 'bo_cfg2')::text AS there;

-- a rename onto a name that is taken is refused rather than silently merging
CREATE TEXT SEARCH CONFIGURATION bo_cfg (COPY = simple);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: text search configuration "bo_cfg2" already exists
-- end-expected-error
ALTER TEXT SEARCH CONFIGURATION bo_cfg RENAME TO bo_cfg2;

DROP TEXT SEARCH CONFIGURATION bo_cfg;
DROP TEXT SEARCH CONFIGURATION bo_cfg2;

DROP TEXT SEARCH DICTIONARY IF EXISTS bo_dict;
DROP TEXT SEARCH DICTIONARY IF EXISTS bo_dict2;
CREATE TEXT SEARCH DICTIONARY bo_dict (TEMPLATE = pg_catalog.simple);
ALTER TEXT SEARCH DICTIONARY bo_dict RENAME TO bo_dict2;

-- begin-expected
-- columns: gone, there
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_ts_dict WHERE dictname = 'bo_dict')::text AS gone,
       (SELECT count(*) FROM pg_ts_dict WHERE dictname = 'bo_dict2')::text AS there;

DROP TEXT SEARCH DICTIONARY bo_dict2;

DROP SERVER IF EXISTS bo_srv CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS bo_fdw CASCADE;
CREATE FOREIGN DATA WRAPPER bo_fdw;
CREATE SERVER bo_srv FOREIGN DATA WRAPPER bo_fdw;
ALTER SERVER bo_srv OPTIONS (ADD host 'h');
ALTER FOREIGN DATA WRAPPER bo_fdw OPTIONS (ADD a 'b');

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_foreign_server WHERE srvname = 'bo_srv';

DROP SERVER bo_srv;
DROP FOREIGN DATA WRAPPER bo_fdw;

-- ALTER EXTENSION on one that is installed is still a no-op
ALTER EXTENSION plpgsql UPDATE;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_extension WHERE extname = 'plpgsql';

-- ============================================================================
-- 4. ALTER SCHEMA on a schema that is not there
-- ============================================================================

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "bo_nosuch" does not exist
-- end-expected-error
ALTER SCHEMA bo_nosuch OWNER TO CURRENT_USER;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "bo_nosuch" does not exist
-- end-expected-error
ALTER SCHEMA bo_nosuch RENAME TO bo_other;

DROP SCHEMA IF EXISTS bo_sch CASCADE;
DROP SCHEMA IF EXISTS bo_sch2 CASCADE;
CREATE SCHEMA bo_sch;
ALTER SCHEMA bo_sch OWNER TO CURRENT_USER;
ALTER SCHEMA bo_sch RENAME TO bo_sch2;

-- begin-expected
-- columns: gone, there
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'bo_sch')::text AS gone,
       (SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'bo_sch2')::text AS there;

DROP SCHEMA bo_sch2;

-- ============================================================================
-- 5. A domain default is a value of the domain
-- ============================================================================
DROP TABLE IF EXISTS bo_dd CASCADE;
DROP DOMAIN IF EXISTS bo_dom CASCADE;
DROP DOMAIN IF EXISTS bo_dtxt CASCADE;
DROP DOMAIN IF EXISTS bo_dnum CASCADE;
DROP DOMAIN IF EXISTS bo_ddate CASCADE;
DROP DOMAIN IF EXISTS bo_dbool CASCADE;
DROP DOMAIN IF EXISTS bo_duuid CASCADE;
CREATE DOMAIN bo_dom AS int;
CREATE DOMAIN bo_dtxt AS text;
CREATE DOMAIN bo_dnum AS numeric(5,2);
CREATE DOMAIN bo_ddate AS date;
CREATE DOMAIN bo_dbool AS boolean;
CREATE DOMAIN bo_duuid AS uuid;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
ALTER DOMAIN bo_dom SET DEFAULT 'abc';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric: "zz"
-- end-expected-error
ALTER DOMAIN bo_dnum SET DEFAULT 'zz';

-- a date says so with its own SQLSTATE
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type date: "nope"
-- end-expected-error
ALTER DOMAIN bo_ddate SET DEFAULT 'nope';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "maybe"
-- end-expected-error
ALTER DOMAIN bo_dbool SET DEFAULT 'maybe';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type uuid: "zzz"
-- end-expected-error
ALTER DOMAIN bo_duuid SET DEFAULT 'zzz';

-- the same check when the domain is created rather than altered
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
CREATE DOMAIN bo_dbad AS int DEFAULT 'abc';

-- what the type does take is taken, and read back as the value it read
ALTER DOMAIN bo_dom SET DEFAULT '7';

-- begin-expected
-- columns: d
-- row: 7
-- end-expected
SELECT domain_default::text AS d FROM information_schema.domains
  WHERE domain_name = 'bo_dom';

-- a quoted integer with spaces around it is still an integer
ALTER DOMAIN bo_dom SET DEFAULT ' 9 ';

-- begin-expected
-- columns: d
-- row: 9
-- end-expected
SELECT domain_default::text AS d FROM information_schema.domains
  WHERE domain_name = 'bo_dom';

-- an unquoted number of the wrong scale is coerced at use, not refused here
ALTER DOMAIN bo_dom SET DEFAULT 1;

-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT domain_default::text AS d FROM information_schema.domains
  WHERE domain_name = 'bo_dom';

-- SET DEFAULT NULL leaves the domain with no default at all
ALTER DOMAIN bo_dom SET DEFAULT NULL;

-- begin-expected
-- columns: d
-- row: <none>
-- end-expected
SELECT coalesce(domain_default, '<none>')::text AS d FROM information_schema.domains
  WHERE domain_name = 'bo_dom';

-- a string domain keeps its default as the quoted literal it was written as
ALTER DOMAIN bo_dtxt SET DEFAULT 'abc';

-- begin-expected
-- columns: d
-- row: 'abc'::text
-- end-expected
SELECT domain_default::text AS d FROM information_schema.domains
  WHERE domain_name = 'bo_dtxt';

-- a boolean default reads back as the value, not as the literal
ALTER DOMAIN bo_dbool SET DEFAULT 'true';

-- begin-expected
-- columns: d
-- row: true
-- end-expected
SELECT domain_default::text AS d FROM information_schema.domains
  WHERE domain_name = 'bo_dbool';

-- and a default the type does take is still what a row gets
CREATE TABLE bo_dd (id int PRIMARY KEY, a bo_dom, b bo_dtxt);
ALTER DOMAIN bo_dom SET DEFAULT 42;
INSERT INTO bo_dd (id) VALUES (1);

-- begin-expected
-- columns: a, b
-- row: 42, abc
-- end-expected
SELECT a::text AS a, b::text AS b FROM bo_dd WHERE id = 1;

DROP TABLE bo_dd;
DROP DOMAIN bo_dom;
DROP DOMAIN bo_dtxt;
DROP DOMAIN bo_dnum;
DROP DOMAIN bo_ddate;
DROP DOMAIN bo_dbool;
DROP DOMAIN bo_duuid;

-- ============================================================================
-- 6. An attribute belongs to the relation a composite type owns
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bo_nosuchtype" does not exist
-- end-expected-error
ALTER TYPE bo_nosuchtype ADD ATTRIBUTE q int;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bo_nosuchtype" does not exist
-- end-expected-error
ALTER TYPE bo_nosuchtype DROP ATTRIBUTE q;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bo_nosuchtype" does not exist
-- end-expected-error
ALTER TYPE bo_nosuchtype ALTER ATTRIBUTE q TYPE text;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bo_nosuchtype" does not exist
-- end-expected-error
ALTER TYPE bo_nosuchtype RENAME ATTRIBUTE q TO r;

-- RENAME TO and SET SCHEMA apply to any type, so they still report a type
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "bo_nosuchtype" does not exist
-- end-expected-error
ALTER TYPE bo_nosuchtype RENAME TO bo_other;

-- an enum owns no relation either, so the attribute forms report the same way
DROP TYPE IF EXISTS bo_en CASCADE;
CREATE TYPE bo_en AS ENUM ('a', 'b');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bo_en" does not exist
-- end-expected-error
ALTER TYPE bo_en ADD ATTRIBUTE q int;

DROP TYPE bo_en;

-- a table owns a relation, but not a composite type's one
DROP TABLE IF EXISTS bo_tt CASCADE;
CREATE TABLE bo_tt (i int PRIMARY KEY);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: is not a composite type
-- end-expected-error
ALTER TYPE bo_tt ADD ATTRIBUTE q int;

DROP TABLE bo_tt;

-- a composite type that is there still takes its attributes
DROP TYPE IF EXISTS bo_ct CASCADE;
CREATE TYPE bo_ct AS (a int);
ALTER TYPE bo_ct ADD ATTRIBUTE q int;

-- begin-expected
-- columns: attrs
-- row: a,q
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum)::text AS attrs FROM pg_attribute
  WHERE attrelid = (SELECT typrelid FROM pg_type WHERE typname = 'bo_ct') AND attnum > 0;

DROP TYPE bo_ct;

-- a domain that is not there is still a missing type, not a missing relation
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "bo_nosuchdomain" does not exist
-- end-expected-error
ALTER DOMAIN bo_nosuchdomain SET NOT NULL;

-- ============================================================================
-- 7. A policy carries the clauses its command has a use for
-- ============================================================================
DROP TABLE IF EXISTS bo_pol CASCADE;
CREATE TABLE bo_pol (id int PRIMARY KEY);

-- CREATE refuses the clause the command cannot use
-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH CHECK cannot be applied to SELECT or DELETE
-- end-expected-error
CREATE POLICY bo_p1 ON bo_pol FOR SELECT WITH CHECK (id > 0);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH CHECK cannot be applied to SELECT or DELETE
-- end-expected-error
CREATE POLICY bo_p2 ON bo_pol FOR DELETE WITH CHECK (id > 0);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: only WITH CHECK expression allowed for INSERT
-- end-expected-error
CREATE POLICY bo_p3 ON bo_pol FOR INSERT USING (id > 0);

-- and takes the ones it can
CREATE POLICY bo_ps ON bo_pol FOR SELECT USING (id > 0);
CREATE POLICY bo_pd ON bo_pol FOR DELETE USING (id > 0);
CREATE POLICY bo_pi ON bo_pol FOR INSERT WITH CHECK (id > 0);
CREATE POLICY bo_pu ON bo_pol FOR UPDATE USING (id > 0) WITH CHECK (id > 1);
CREATE POLICY bo_pa ON bo_pol FOR ALL USING (id > 0) WITH CHECK (id > 1);

-- ALTER cannot change the command, so the same rule holds there
-- begin-expected-error
-- sqlstate: 42601
-- message-like: only USING expression allowed for SELECT, DELETE
-- end-expected-error
ALTER POLICY bo_ps ON bo_pol WITH CHECK (id > 1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: only USING expression allowed for SELECT, DELETE
-- end-expected-error
ALTER POLICY bo_ps ON bo_pol USING (id > 2) WITH CHECK (id > 2);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: only USING expression allowed for SELECT, DELETE
-- end-expected-error
ALTER POLICY bo_pd ON bo_pol WITH CHECK (id > 1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: only WITH CHECK expression allowed for INSERT
-- end-expected-error
ALTER POLICY bo_pi ON bo_pol USING (id > 1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: only WITH CHECK expression allowed for INSERT
-- end-expected-error
ALTER POLICY bo_pi ON bo_pol USING (id > 2) WITH CHECK (id > 2);

-- what each command can take, ALTER still takes
ALTER POLICY bo_ps ON bo_pol USING (id > 5);
ALTER POLICY bo_pd ON bo_pol USING (id > 5);
ALTER POLICY bo_pi ON bo_pol WITH CHECK (id > 5);
ALTER POLICY bo_pu ON bo_pol USING (id > 5) WITH CHECK (id > 6);
ALTER POLICY bo_pa ON bo_pol USING (id > 5) WITH CHECK (id > 6);
ALTER POLICY bo_pu ON bo_pol TO CURRENT_USER;
ALTER POLICY bo_ps ON bo_pol RENAME TO bo_ps2;

-- begin-expected
-- columns: policyname, cmd, q, w
-- row: bo_pa, ALL, t, t
-- row: bo_pd, DELETE, t, f
-- row: bo_pi, INSERT, f, t
-- row: bo_ps2, SELECT, t, f
-- row: bo_pu, UPDATE, t, t
-- end-expected
SELECT policyname::text, cmd::text, (qual IS NOT NULL)::text AS q,
       (with_check IS NOT NULL)::text AS w
  FROM pg_policies WHERE tablename = 'bo_pol' ORDER BY policyname;

DROP TABLE bo_pol;

-- ============================================================================
-- 8. A table an inheritance child depends on
-- ============================================================================
DROP TABLE IF EXISTS bo_ic CASCADE;
DROP TABLE IF EXISTS bo_ip CASCADE;
CREATE TABLE bo_ip (i int PRIMARY KEY);
CREATE TABLE bo_ic () INHERITS (bo_ip);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table bo_ip because other objects depend on it
-- detail-like: table bo_ic depends on table bo_ip
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE bo_ip;

-- the parent is still there, and so is the child
-- begin-expected
-- columns: p, c
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM information_schema.tables
         WHERE table_name = 'bo_ip')::text AS p,
       (SELECT count(*) FROM information_schema.tables
         WHERE table_name = 'bo_ic')::text AS c;

-- CASCADE takes the child with it (and says so in a NOTICE the harness
-- does not read)
DROP TABLE bo_ip CASCADE;

-- begin-expected
-- columns: p, c
-- row: 0, 0
-- end-expected
SELECT (SELECT count(*) FROM information_schema.tables
         WHERE table_name = 'bo_ip')::text AS p,
       (SELECT count(*) FROM information_schema.tables
         WHERE table_name = 'bo_ic')::text AS c;

-- a view over the table says which view depends on it
DROP VIEW IF EXISTS bo_vv;
DROP TABLE IF EXISTS bo_vt CASCADE;
CREATE TABLE bo_vt (i int PRIMARY KEY);
CREATE VIEW bo_vv AS SELECT i FROM bo_vt;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table bo_vt because other objects depend on it
-- detail-like: view bo_vv depends on table bo_vt
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE bo_vt;

DROP TABLE bo_vt CASCADE;

-- begin-expected
-- columns: t, v
-- row: 0, 0
-- end-expected
SELECT (SELECT count(*) FROM information_schema.tables
         WHERE table_name = 'bo_vt')::text AS t,
       (SELECT count(*) FROM information_schema.views
         WHERE table_name = 'bo_vv')::text AS v;

-- a child that no longer inherits is no longer a dependent
DROP TABLE IF EXISTS bo_ic CASCADE;
DROP TABLE IF EXISTS bo_ip CASCADE;
CREATE TABLE bo_ip (i int PRIMARY KEY);
CREATE TABLE bo_ic () INHERITS (bo_ip);
ALTER TABLE bo_ic NO INHERIT bo_ip;
DROP TABLE bo_ip;

-- begin-expected
-- columns: p, c
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*) FROM information_schema.tables
         WHERE table_name = 'bo_ip')::text AS p,
       (SELECT count(*) FROM information_schema.tables
         WHERE table_name = 'bo_ic')::text AS c;

DROP TABLE bo_ic;

-- an ordinary table with nothing depending on it is dropped as before
DROP TABLE IF EXISTS bo_plain CASCADE;
CREATE TABLE bo_plain (i int PRIMARY KEY);
DROP TABLE bo_plain;

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM information_schema.tables WHERE table_name = 'bo_plain';
