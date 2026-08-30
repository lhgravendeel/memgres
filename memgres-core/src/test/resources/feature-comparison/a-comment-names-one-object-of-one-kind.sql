CREATE TABLE zf_t (id int, v text);
CREATE VIEW zf_vw AS SELECT id FROM zf_t;
CREATE SEQUENCE zf_sq;
CREATE INDEX zf_ix ON zf_t (id);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_t" is not a view
-- end-expected-error
COMMENT ON VIEW zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_t" is not an index
-- end-expected-error
COMMENT ON INDEX zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_t" is not a sequence
-- end-expected-error
COMMENT ON SEQUENCE zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_t" is not a materialized view
-- end-expected-error
COMMENT ON MATERIALIZED VIEW zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_vw" is not a table
-- end-expected-error
COMMENT ON TABLE zf_vw IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_sq" is not a table
-- end-expected-error
COMMENT ON TABLE zf_sq IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_ix" is not a table
-- end-expected-error
COMMENT ON TABLE zf_ix IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_nosuchrel" does not exist
-- end-expected-error
COMMENT ON VIEW zf_nosuchrel IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_nosuchrel" does not exist
-- end-expected-error
COMMENT ON INDEX zf_nosuchrel IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_nosuchrel" does not exist
-- end-expected-error
COMMENT ON SEQUENCE zf_nosuchrel IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_nosuchrel" does not exist
-- end-expected-error
COMMENT ON MATERIALIZED VIEW zf_nosuchrel IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zf_t" is not a foreign table
-- end-expected-error
COMMENT ON FOREIGN TABLE zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "BOGUSKIND"
-- end-expected-error
COMMENT ON BOGUSKIND zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "zf_t"
-- end-expected-error
COMMENT ON zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
COMMENT ON TABLE zf_t, zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
COMMENT ON TABLE zf_t IS 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "true"
-- end-expected-error
COMMENT ON TABLE zf_t IS true;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DEFAULT"
-- end-expected-error
COMMENT ON TABLE zf_t IS DEFAULT;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "||"
-- end-expected-error
COMMENT ON TABLE zf_t IS 'a' || 'b';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'y'"
-- end-expected-error
COMMENT ON TABLE zf_t IS 'x' 'y';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXTRA"
-- end-expected-error
COMMENT ON TABLE zf_t IS 'x' EXTRA;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
COMMENT ON TABLE zf_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "IS"
-- end-expected-error
COMMENT ON TABLE IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "TABLE"
-- end-expected-error
COMMENT TABLE zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: column name must be qualified
-- end-expected-error
COMMENT ON COLUMN zf_t IS 'x';
COMMENT ON COLUMN zf_t.id IS 'the id';
-- begin-expected
-- columns: d
-- row: the id
-- end-expected
SELECT col_description('zf_t'::regclass, 1) AS d;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zf_t" does not exist
-- end-expected-error
COMMENT ON COLUMN zf_t.nosuchcol IS 'x';
COMMENT ON COLUMN public.zf_t.v IS 'the v';
-- begin-expected
-- columns: d
-- row: the v
-- end-expected
SELECT col_description('zf_t'::regclass, 2) AS d;
COMMENT ON TABLE zf_t IS NULL;
COMMENT ON TABLE zf_t IS $$dollar$$;
-- begin-expected
-- columns: d
-- row: dollar
-- end-expected
SELECT obj_description('zf_t'::regclass) AS d;
COMMENT ON CAST (int4 AS int8) IS 'widening';
-- begin-expected
-- columns: d
-- row: widening
-- end-expected
SELECT obj_description(oid,'pg_cast') AS d FROM pg_cast WHERE castsource='int4'::regtype AND casttarget='int8'::regtype;
COMMENT ON CAST (int4 AS int8) IS NULL;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zf_nosuchtype" does not exist
-- end-expected-error
COMMENT ON CAST (int AS zf_nosuchtype) IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: cast from type integer to type xml does not exist
-- end-expected-error
COMMENT ON CAST (int4 AS xml) IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
COMMENT ON CAST (int) IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "int"
-- end-expected-error
COMMENT ON CAST int AS int8 IS 'x';
CREATE FUNCTION zf_add(int,int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT $1+$2 $$;
CREATE OPERATOR ###@ (LEFTARG=int, RIGHTARG=int, FUNCTION=zf_add);
COMMENT ON OPERATOR ###@ (int,int) IS 'adds them';
-- begin-expected
-- columns: d
-- row: adds them
-- end-expected
SELECT obj_description(oid,'pg_operator') AS d FROM pg_operator WHERE oprname='###@';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer #%^&* integer
-- end-expected-error
COMMENT ON OPERATOR #%^&* (int, int) IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "IS"
-- end-expected-error
COMMENT ON OPERATOR ###@ IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zf_nosuchtype" does not exist
-- end-expected-error
COMMENT ON OPERATOR = (zf_nosuchtype, int) IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "zf_nosuchcoll" for encoding "UTF8" does not exist
-- end-expected-error
COMMENT ON COLLATION zf_nosuchcoll IS 'x';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: aggregate zf_nosuchagg(integer) does not exist
-- end-expected-error
COMMENT ON AGGREGATE zf_nosuchagg(int) IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "IS"
-- end-expected-error
COMMENT ON AGGREGATE zf_nosuchagg IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "zf_nosuch" does not exist
-- end-expected-error
COMMENT ON TEXT SEARCH CONFIGURATION zf_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: tablespace "zf_nosuchts" does not exist
-- end-expected-error
COMMENT ON TABLESPACE zf_nosuchts IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator class "zf_nosuch" does not exist for access method "btree"
-- end-expected-error
COMMENT ON OPERATOR CLASS zf_nosuch USING btree IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator family "zf_nosuch" does not exist for access method "btree"
-- end-expected-error
COMMENT ON OPERATOR FAMILY zf_nosuch USING btree IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: access method "zf_nosuch" does not exist
-- end-expected-error
COMMENT ON ACCESS METHOD zf_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: server "zf_nosuch" does not exist
-- end-expected-error
COMMENT ON SERVER zf_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: publication "zf_nosuch" does not exist
-- end-expected-error
COMMENT ON PUBLICATION zf_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: transform for type integer language "sql" does not exist
-- end-expected-error
COMMENT ON TRANSFORM FOR int LANGUAGE sql IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zf_nosuchdom" does not exist
-- end-expected-error
COMMENT ON DOMAIN zf_nosuchdom IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zf_nosuchtype" does not exist
-- end-expected-error
COMMENT ON TYPE zf_nosuchtype IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_nosuchrel" does not exist
-- end-expected-error
COMMENT ON CONSTRAINT c ON zf_nosuchrel IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: trigger "tg" for table "zf_t" does not exist
-- end-expected-error
COMMENT ON TRIGGER tg ON zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: rule "r" for relation "zf_t" does not exist
-- end-expected-error
COMMENT ON RULE r ON zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: policy "p" for table "zf_t" does not exist
-- end-expected-error
COMMENT ON POLICY p ON zf_t IS 'x';
CREATE ROLE zf_r NOLOGIN;
COMMENT ON ROLE zf_r IS 'the role';
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_shdescription WHERE description='the role';
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_description WHERE description='the role';
CREATE TABLE zf_t2 (a int, b int);
CREATE STATISTICS zf_st ON a,b FROM zf_t2;
COMMENT ON STATISTICS zf_st IS 'correlated';
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_description WHERE description='correlated';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: statistics object "zf_nosuchst" does not exist
-- end-expected-error
COMMENT ON STATISTICS zf_nosuchst IS 'x';
BEGIN;
COMMENT ON TABLE zf_t IS 'rolled back';
ROLLBACK;
-- begin-expected
-- columns: undone
-- row: f
-- end-expected
SELECT obj_description('zf_t'::regclass) IS NULL AS undone;
CREATE TABLE "zf_Cap" (id int);
COMMENT ON TABLE "zf_Cap" IS 'kept';
-- begin-expected
-- columns: d
-- row: kept
-- end-expected
SELECT obj_description('"zf_Cap"'::regclass) AS d;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_description WHERE description='kept';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_cap" does not exist
-- end-expected-error
COMMENT ON TABLE zf_cap IS 'x';
CREATE TABLE zf_c3 (a int, b int, c int);
ALTER TABLE zf_c3 DROP COLUMN b;
COMMENT ON COLUMN zf_c3.c IS 'ccc';
-- begin-expected
-- columns: objsubid
-- row: 3
-- end-expected
SELECT objsubid FROM pg_description WHERE objoid='zf_c3'::regclass;
-- begin-expected
-- columns: at3 | at2
-- row: ccc | NULL
-- end-expected
SELECT col_description('zf_c3'::regclass,3) AS at3, col_description('zf_c3'::regclass,2) AS at2;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: no security label providers have been loaded
-- end-expected-error
SECURITY LABEL ON TABLE zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: security label provider "anything" is not loaded
-- end-expected-error
SECURITY LABEL FOR anything ON TABLE zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "zf_t"
-- end-expected-error
SECURITY LABEL ON zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "IS"
-- end-expected-error
SECURITY LABEL ON TABLE IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SECURITY LABEL ON TABLE zf_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SECURITY LABEL ON TABLE zf_t IS;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
SECURITY LABEL ON TABLE zf_t IS 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ON"
-- end-expected-error
SECURITY LABEL FOR ON TABLE zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "TRIGGER"
-- end-expected-error
SECURITY LABEL ON TRIGGER tr ON zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "OPERATOR"
-- end-expected-error
SECURITY LABEL ON OPERATOR + (int, int) IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "BOGUS"
-- end-expected-error
SECURITY LABEL ON BOGUS zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: no security label providers have been loaded
-- end-expected-error
SECURITY LABEL ON COLUMN zf_t.id IS 'x';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: no security label providers have been loaded
-- end-expected-error
SECURITY LABEL ON COLUMN zf_t IS 'x';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: no security label providers have been loaded
-- end-expected-error
SECURITY LABEL ON TABLE zf_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: no security label providers have been loaded
-- end-expected-error
SECURITY LABEL ON TABLE zf_t IS NULL;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: no security label providers have been loaded
-- end-expected-error
SECURITY LABEL ON ROLE zf_nosuchrole IS 'x';
DROP OPERATOR ###@ (int,int);
DROP FUNCTION zf_add(int,int);
DROP ROLE zf_r;
DROP VIEW zf_vw;
DROP SEQUENCE zf_sq;
DROP TABLE zf_t, zf_t2, zf_c3, "zf_Cap" CASCADE;
