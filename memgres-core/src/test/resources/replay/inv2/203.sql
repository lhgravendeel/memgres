-- source: investigation-2026-08.md
-- finding: 203
-- title: COMMENT is one if/else chain over object kinds that resolves almost nothing: 21 kinds have no arm at all and fall through to success, the relation arms do not c
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_ct AS (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON COLUMN zz_ct.a IS 'ctcol';
-- begin-expected
-- columns: col_description:text
-- row: ctcol
-- rowcount: 1
-- end-expected
SELECT col_description('zz_ct'::regclass, 1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_nosuch" does not exist
-- end-expected-error
COMMENT ON SEQUENCE zz_vf2_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_nosuch" does not exist
-- end-expected-error
COMMENT ON MATERIALIZED VIEW zz_vf2_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_vf2_nosuch" does not exist
-- end-expected-error
COMMENT ON ROLE zz_vf2_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: extension "zz_vf2_nosuch" does not exist
-- end-expected-error
COMMENT ON EXTENSION zz_vf2_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "zz_vf2_nosuch" does not exist
-- end-expected-error
COMMENT ON LANGUAGE zz_vf2_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "zz_vf2_nosuch" for encoding "UTF8" does not exist
-- end-expected-error
COMMENT ON COLLATION zz_vf2_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: event trigger "zz_vf2_nosuch" does not exist
-- end-expected-error
COMMENT ON EVENT TRIGGER zz_vf2_nosuch IS 'x';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: aggregate zz_vf2_nosuch(integer) does not exist
-- end-expected-error
COMMENT ON AGGREGATE zz_vf2_nosuch(int) IS 'x';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: large object 987654321 does not exist
-- end-expected-error
COMMENT ON LARGE OBJECT 987654321 IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE VIEW zz_v AS SELECT id FROM zz_t;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_v" does not exist
-- end-expected-error
COMMENT ON TABLE zz_v IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_s" is not a table
-- end-expected-error
COMMENT ON TABLE zz_s IS 'x';
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1';
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_p() LANGUAGE sql AS 'SELECT 1';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_f(text) does not exist
-- end-expected-error
COMMENT ON FUNCTION zz_f(text) IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: function zz_f(integer) is not an aggregate
-- end-expected-error
COMMENT ON AGGREGATE zz_f(int) IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: zz_p() is not a function
-- end-expected-error
COMMENT ON FUNCTION zz_p() IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: zz_f(integer) is not a procedure
-- end-expected-error
COMMENT ON PROCEDURE zz_f(int) IS 'x';
