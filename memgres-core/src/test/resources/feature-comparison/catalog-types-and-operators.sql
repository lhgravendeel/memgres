DROP TABLE IF EXISTS zz_cy_t CASCADE;

DROP TABLE IF EXISTS zz_cy_v CASCADE;

DROP TABLE IF EXISTS zz_cy_d CASCADE;

DROP OPERATOR IF EXISTS ##@ (integer, integer);

DROP FUNCTION IF EXISTS zz_cy_add(integer, integer);

DROP AGGREGATE IF EXISTS zz_cy_ag(int);

DROP FUNCTION IF EXISTS zz_cy_sf(int, int);

DROP COLLATION IF EXISTS zz_cy_coll;

CREATE FUNCTION zz_cy_add(a integer, b integer) RETURNS integer LANGUAGE sql AS $$ SELECT a * 100 + b $$;

CREATE OPERATOR ##@ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = zz_cy_add);

CREATE TABLE zz_cy_t (x int);

INSERT INTO zz_cy_t VALUES (1);

CREATE FUNCTION zz_cy_sf(int, int) RETURNS int LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT $1 * 10 + $2 $$;

CREATE AGGREGATE zz_cy_ag (int) (SFUNC = zz_cy_sf, STYPE = int);

CREATE TABLE zz_cy_v (id int);

CREATE TABLE zz_cy_d (id int, d_null int DEFAULT NULL, d_val int DEFAULT 7);

-- begin-expected
-- columns: ?column?
-- row: 102
-- end-expected
SELECT 1 ##@ 2;

-- begin-expected
-- columns: ?column?
-- row: 102
-- end-expected
SELECT 1::smallint ##@ 2::smallint;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: numeric ##@ numeric
-- end-expected-error
SELECT 1.5 ##@ 2.5;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: bigint ##@ bigint
-- end-expected-error
SELECT 1::bigint ##@ 2::bigint;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT 'a' ##@ 'b';

-- begin-expected
-- columns: ?column?
-- row: 101
-- end-expected
SELECT x ##@ 1 FROM zz_cy_t;

-- begin-expected
-- columns: pg_typeof|pg_typeof|pg_typeof
-- row: oid|name|oid
-- end-expected
SELECT pg_typeof(oid)::text, pg_typeof(datname)::text, pg_typeof(datdba)::text FROM pg_database WHERE datname='template1';

-- begin-expected
-- columns: pg_typeof|pg_typeof|pg_typeof|pg_typeof|pg_typeof
-- row: oid|name|oid|oid[]|text[]
-- end-expected
SELECT pg_typeof(oid)::text, pg_typeof(extname)::text, pg_typeof(extowner)::text, pg_typeof(extconfig)::text, pg_typeof(extcondition)::text FROM pg_extension WHERE extname='plpgsql';

-- begin-expected
-- columns: pg_typeof|pg_typeof|pg_typeof
-- row: oid|name|oid
-- end-expected
SELECT pg_typeof(oid)::text, pg_typeof(lanname)::text, pg_typeof(lanplcallfoid)::text FROM pg_language WHERE lanname='plpgsql';

-- begin-expected
-- columns: pg_typeof
-- row: text[]
-- end-expected
SELECT pg_typeof(rolconfig)::text FROM pg_roles WHERE rolname='pg_monitor';

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: cannot insert into view "pg_cursors"
-- end-expected-error
INSERT INTO pg_cursors VALUES ('x','y',false,false,false,now());

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: cannot delete from view "pg_cursors"
-- end-expected-error
DELETE FROM pg_cursors;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: cannot update view "pg_cursors"
-- end-expected-error
UPDATE pg_cursors SET name = 'z';

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: cannot delete from view "pg_settings"
-- end-expected-error
DELETE FROM pg_settings;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: function zz_cy_ag() does not exist
-- end-expected-error
SELECT zz_cy_ag() FROM (VALUES (1),(2)) t(v);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: ANALYZE option must be specified when a column list is provided
-- end-expected-error
VACUUM zz_cy_v (id);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: ANALYZE option must be specified when a column list is provided
-- end-expected-error
VACUUM zz_cy_v (nosuchcol);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: ANALYZE option must be specified when a column list is provided
-- end-expected-error
VACUUM (ANALYZE FALSE) zz_cy_v (id);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ")"
-- end-expected-error
VACUUM zz_cy_v ();

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::int FROM pg_attrdef d JOIN pg_class c ON c.oid=d.adrelid WHERE c.relname='zz_cy_d';

-- begin-expected
-- columns: attname|atthasdef
-- row: id|f
-- row: d_null|f
-- row: d_val|t
-- end-expected
SELECT attname::text, atthasdef FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='zz_cy_d' AND attnum > 0 ORDER BY attnum;

CREATE COLLATION zz_cy_coll (LOCALE = 'C');

-- begin-expected-error
-- sqlstate: 42710
-- message-like: ERROR: collation "zz_cy_coll" for encoding "UTF8" already exists
-- end-expected-error
CREATE COLLATION zz_cy_coll (LOCALE = 'C');

DROP COLLATION zz_cy_coll;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::int FROM pg_collation WHERE collname = 'zz_cy_coll';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: collation "zz_cy_coll" for encoding "UTF8" does not exist
-- end-expected-error
SELECT 'a' COLLATE zz_cy_coll;

DROP TABLE zz_cy_d;

DROP TABLE zz_cy_v;

DROP AGGREGATE zz_cy_ag(int);

DROP FUNCTION zz_cy_sf(int, int);

DROP TABLE zz_cy_t;

DROP OPERATOR ##@ (integer, integer);

DROP FUNCTION zz_cy_add(integer, integer);

