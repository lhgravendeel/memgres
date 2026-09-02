-- source: review-2026-08.md
-- finding: Root cause 13: CREATE EXTENSION has no availability list, no version registry, no dependency record and no undo entry
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 13: CREATE EXTENSION has no availability list, no version registry, no dependency record and no undo entry
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: extension "zz_noext" is not available
-- end-expected-error
CREATE EXTENSION zz_noext;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: extension "zz_noext" is not available
-- end-expected-error
CREATE EXTENSION IF NOT EXISTS zz_noext;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: extension "zz_noext" is not available
-- end-expected-error
CREATE EXTENSION zz_noext CASCADE;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: extension "zz_noext" is not available
-- end-expected-error
CREATE EXTENSION zz_noext VERSION '1.0';
-- begin-expected
-- ok: 0
-- end-expected
CREATE EXTENSION pgcrypto;
-- already installed
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: extension "zz_noext2" is not available
-- end-expected-error
CREATE EXTENSION IF NOT EXISTS zz_noext2 VERSION '99.99';
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: extension "zz_noext3" is not available
-- end-expected-error
CREATE EXTENSION IF NOT EXISTS zz_noext3 WITH SCHEMA zz_nosuchschema;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: extension "zz_noext" does not exist
-- end-expected-error
DROP EXTENSION zz_noext;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: extension "pgcrypto" has no update path from version "1.4" to version "99.99"
-- end-expected-error
ALTER EXTENSION pgcrypto UPDATE TO '99.99';
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_nosuchschema" does not exist
-- end-expected-error
ALTER EXTENSION pgcrypto SET SCHEMA zz_nosuchschema;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_notable" does not exist
-- end-expected-error
ALTER EXTENSION pgcrypto ADD TABLE zz_notable;
-- begin-expected
-- ok: 0
-- end-expected
CREATE EXTENSION citext;
-- begin-expected
-- columns: nspname:name | extrelocatable:bool
-- row: public | t
-- rowcount: 1
-- end-expected
SELECT n.nspname, e.extrelocatable FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname='citext';
-- begin-expected-error
-- sqlstate: 42710
-- message-like: extension "citext" already exists
-- end-expected-error
CREATE EXTENSION citext;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT count(*) > 0 FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid
 WHERE e.extname='citext' AND d.deptype='e';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE EXTENSION IF NOT EXISTS citext;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: count:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_extension WHERE extname='citext';
