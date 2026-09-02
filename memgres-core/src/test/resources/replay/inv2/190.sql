-- source: investigation-2026-08.md
-- finding: 190
-- title: search_path is treated as a list of names with public as a floor: entries are never checked against the catalog, public is appended when nothing else resolves, 
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = zz_vf_nosuch, public;
-- begin-expected
-- columns: current_schemas:text
-- row: {public}
-- rowcount: 1
-- end-expected
SELECT current_schemas(false)::text;
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = public, pg_catalog;
-- begin-expected
-- columns: current_schemas:text
-- row: {public,pg_catalog}
-- rowcount: 1
-- end-expected
SELECT current_schemas(true)::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_vf_qt (a int);
-- begin-expected
-- columns: array_length:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT array_length(current_schemas(true), 1);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT current_schemas(true)::text LIKE '{pg_temp%';
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = pg_temp, public;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT current_schema() LIKE 'pg\_temp%';
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = '';
-- begin-expected
-- columns: current_schema:name
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT current_schema();
-- begin-expected
-- columns: current_schemas:text
-- row: {}
-- rowcount: 1
-- end-expected
SELECT current_schemas(false)::text;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: no schema has been selected to create in
-- end-expected-error
CREATE TABLE zz_vf_q8 (a int);
