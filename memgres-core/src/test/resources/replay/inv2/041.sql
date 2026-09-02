-- source: investigation-2026-08.md
-- finding: 41
-- title: Unrelated singletons in this area
-- begin-expected
-- columns: jsonb_agg_strict:text
-- row: [1]
-- rowcount: 1
-- end-expected
SELECT jsonb_agg_strict(v)::text FROM (VALUES (1),(NULL::int)) t(v);
-- begin-expected
-- columns: json_agg_strict:text
-- row: [10, 20]
-- rowcount: 1
-- end-expected
SELECT json_agg_strict(v)::text FROM (VALUES (10),(20)) t(v);
-- begin-expected
-- columns: json_object_agg_strict:text
-- row: { "a" : 20 }
-- rowcount: 1
-- end-expected
SELECT json_object_agg_strict(k, v)::text FROM (VALUES ('a',20),('c',NULL::int)) t(k,v);
-- begin-expected
-- columns: jsonb_object_agg_unique:text
-- row: {"a": 20}
-- rowcount: 1
-- end-expected
SELECT jsonb_object_agg_unique(k, v)::text FROM (VALUES ('a',20)) t(k,v);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '{"a":1}'::jsonb ? 'a' IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 1 = 1 IS NULL;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE IF EXISTS zz_vf_agg;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_agg (k text, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_agg VALUES ('a',20),('b',10),('c',NULL);
-- begin-expected
-- columns: json_objectagg:text
-- row: { "a" : 20, "b" : 10, "c" : null }
-- rowcount: 1
-- end-expected
SELECT JSON_OBJECTAGG(k VALUE v)::text FROM (SELECT k,v FROM zz_vf_agg ORDER BY k) s;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE IF EXISTS zz_vf_agg;
-- begin-expected
-- columns: jsonb_set_lax:text
-- row: {"a": {"c": 2}}
-- rowcount: 1
-- end-expected
SELECT jsonb_set_lax('{"a":{"b":1,"c":2}}', '{a,b}', NULL, true, 'delete_key')::text;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: null_value_treatment must be "delete_key", "return_target", "use_json_null", or "raise_exception"
-- end-expected-error
SELECT jsonb_set_lax('{"a":1}','{a}',NULL,true,'bogus')::text;
-- begin-expected
-- columns: jsonb_set_lax:text
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_set_lax('{"a":1}','{a}',NULL,true,'return_target')::text;
-- begin-expected
-- columns: to_tsvector:text
-- row: 'cat':1
-- rowcount: 1
-- end-expected
SELECT to_tsvector('english', '{"a": "cats"}'::json)::text;
-- begin-expected
-- columns: ts_headline:text
-- row: {"a":"<b>cats</b> and dogs"}
-- rowcount: 1
-- end-expected
SELECT ts_headline('english', '{"a":"cats and dogs"}'::json, to_tsquery('english','cat'))::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE EXTENSION IF NOT EXISTS hstore;
-- begin-expected
-- columns: hstore_to_json_loose:text
-- row: {"a": "+5"}
-- rowcount: 1
-- end-expected
SELECT hstore_to_json_loose('a=>+5'::hstore)::text;
-- begin-expected
-- columns: hstore_to_json_loose:text
-- row: {"a": ".5"}
-- rowcount: 1
-- end-expected
SELECT hstore_to_json_loose('a=>.5'::hstore)::text;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT '{'::json;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT '{"a":}'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT 'notjson'::json;
