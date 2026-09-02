-- source: review-2026-08.md
-- finding: Root cause 6: the populate/to_record family guesses value types from the JSON spelling
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 6: the populate/to_record family guesses value types from the JSON spelling
-- begin-expected
-- ok: 0
-- end-expected
DROP TYPE IF EXISTS zz_rt CASCADE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_rt AS (a int, b text, c boolean, arr int[]);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "notint"
-- end-expected-error
SELECT a   FROM json_populate_record(null::zz_rt, '{"a":"notint"}'::json);
-- begin-expected
-- columns: c:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT c   FROM json_populate_record(null::zz_rt, '{"c":"yes"}'::json);
-- begin-expected
-- columns: arr:_int4
-- row: {3,4}
-- rowcount: 1
-- end-expected
SELECT arr FROM json_populate_record(null::zz_rt, '{"arr":[3,4]}'::json);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "xyz"
-- end-expected-error
SELECT *   FROM jsonb_to_recordset('[{"a":"xyz"}]'::jsonb) AS t(a int);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "true"
-- end-expected-error
SELECT *   FROM jsonb_to_recordset('[{"a":true}]'::jsonb)  AS t(a int);
-- begin-expected
-- ok: 0
-- end-expected
DROP TYPE IF EXISTS zz_rt CASCADE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_rt AS (a int, b text, c boolean, arr int[]);
-- begin-expected
-- columns: a:int4 | b:text
-- row: 1 | d
-- rowcount: 1
-- end-expected
SELECT a, b FROM json_populate_record(row(9,'d',true,null)::zz_rt, '{"a":1}'::json);
-- begin-expected
-- columns: a:int4
-- row: 9
-- rowcount: 1
-- end-expected
SELECT a    FROM json_populate_record(row(9,'d',true,null)::zz_rt, NULL::json);
-- begin-expected
-- ok: 0
-- end-expected
DROP TYPE IF EXISTS zz_rt CASCADE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_rt AS (a int, b text);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call json_populate_recordset on an object
-- end-expected-error
SELECT a FROM json_populate_recordset(null::zz_rt, '{"a":1}'::json);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call populate_composite on an array
-- end-expected-error
SELECT a FROM json_populate_record(null::zz_rt, '[1,2]'::json);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call populate_composite on a scalar
-- end-expected-error
SELECT a FROM json_populate_record(null::zz_rt, '5'::json);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call json_to_recordset on an object
-- end-expected-error
SELECT * FROM json_to_recordset('{"a":1}'::json) AS t(a int);
