-- ============================================================================
-- -- A document carries the type it was given, wherever it is passed.
-- --
-- -- json and jsonb are two types and neither is read as the other, so a COALESCE, GREATEST or
-- -- LEAST that names both has no type its whole list shares -- and PostgreSQL settles that from
-- -- the arguments' declared types before it evaluates any of them, so an aggregate over no rows
-- -- fails the same way an aggregate over many would. The same declared type is what says whether
-- -- an array's elements are documents to be written into a larger one or the characters they
-- -- happen to hold, and what says that a document handed to the text search functions comes back
-- -- a document with its matches marked inside it rather than the text those braces spell.
--
-- ============================================================================

-- json and jsonb are two types, and a list that names both has no type at all
-- begin-expected-error
-- sqlstate: 42846
-- message-like: COALESCE could not convert type jsonb to json
-- end-expected-error
SELECT coalesce('{}'::json, '[]'::jsonb)::text AS a;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: COALESCE could not convert type json to jsonb
-- end-expected-error
SELECT coalesce('{}'::jsonb, '[]'::json)::text AS a;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: GREATEST could not convert type jsonb to json
-- end-expected-error
SELECT greatest('{}'::json, '[]'::jsonb)::text AS a;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: LEAST could not convert type jsonb to json
-- end-expected-error
SELECT least('{}'::json, '[]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: {"a":1}
-- end-expected
SELECT coalesce('{"a":1}'::json, '[]'::json)::text AS a;
-- begin-expected
-- columns: a
-- row: {"a": 1, "b": 2}
-- end-expected
SELECT coalesce('{"b":2,"a":1}'::jsonb, '[]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: []
-- end-expected
SELECT coalesce(NULL::jsonb, '[]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: []
-- end-expected
SELECT coalesce(NULL::json, NULL::json, '[]'::json)::text AS a;
-- begin-expected
-- columns: a
-- row: [2]
-- end-expected
SELECT greatest('[1]'::jsonb, '[2]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: [1]
-- end-expected
SELECT least('[1]'::jsonb, '[2]'::jsonb)::text AS a;
-- the type of the whole is settled before any argument is evaluated, so an
-- aggregate that found nothing still fails the list it does not go with
DROP TABLE IF EXISTS jt_docs CASCADE;
CREATE TABLE jt_docs (id int, v json, name text);
-- begin-expected
-- columns: a
-- row: json
-- end-expected
SELECT pg_typeof(json_agg(v))::text AS a FROM jt_docs;
-- begin-expected
-- columns: a
-- row: json
-- end-expected
SELECT pg_typeof(JSON_ARRAYAGG(v))::text AS a FROM jt_docs;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: COALESCE could not convert type jsonb to json
-- end-expected-error
SELECT coalesce(json_agg(v), '[]'::jsonb)::text AS a FROM jt_docs;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: COALESCE could not convert type jsonb to json
-- end-expected-error
SELECT coalesce(JSON_ARRAYAGG(v), '[]'::jsonb)::text AS a FROM jt_docs;
-- begin-expected
-- columns: a
-- row: []
-- end-expected
SELECT coalesce(json_agg(v), '[]'::json)::text AS a FROM jt_docs;
INSERT INTO jt_docs VALUES (1, '{"a":1}', 'ab'), (2, '{"a":2}', NULL), (3, '{"a":1}', 'ab');
-- begin-expected
-- columns: a
-- row: [{"a":1}, {"a":2}, {"a":1}]
-- end-expected
SELECT coalesce(json_agg(v), '[]'::json)::text AS a FROM jt_docs;
-- DISTINCT collapses the arguments to the values they are, and a null is one of them
-- begin-expected
-- columns: a
-- row: ["ab", null]
-- end-expected
SELECT json_agg(DISTINCT name ORDER BY name)::text AS a FROM jt_docs;
-- begin-expected
-- columns: a
-- row: ["ab", null]
-- end-expected
SELECT jsonb_agg(DISTINCT name ORDER BY name)::text AS a FROM jt_docs;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type json
-- end-expected-error
SELECT json_agg(DISTINCT v)::text AS a FROM jt_docs;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(DISTINCT name)::text AS a FROM jt_docs;
-- An element of an array is written as what its element type says it is
-- begin-expected
-- columns: a
-- row: [1,2]
-- end-expected
SELECT array_to_json(ARRAY[1,2])::text AS a;
-- begin-expected
-- columns: a
-- row: [[1,2],[3,4]]
-- end-expected
SELECT array_to_json(ARRAY[[1,2],[3,4]])::text AS a;
-- begin-expected
-- columns: a
-- row: [{"a":1},[2]]
-- end-expected
SELECT array_to_json(ARRAY['{"a":1}'::json,'[2]'::json])::text AS a;
-- begin-expected
-- columns: a
-- row: [{"a": 1},[2]]
-- end-expected
SELECT array_to_json(ARRAY['{"a":1}'::jsonb,'[2]'::jsonb])::text AS a;
-- begin-expected
-- columns: a
-- row: ["{1,2}","x"]
-- end-expected
SELECT array_to_json(ARRAY['{1,2}','x'])::text AS a;
-- begin-expected
-- columns: a
-- row: [1,2]
-- end-expected
SELECT array_to_json('{1,2}'::int[])::text AS a;
-- begin-expected
-- columns: a
-- row: ["a","b"]
-- end-expected
SELECT array_to_json(ARRAY['a','b'])::text AS a;
-- begin-expected
-- columns: a
-- row: [{"a":1}]
-- end-expected
SELECT to_json(ARRAY['{"a":1}'::json])::text AS a;
-- begin-expected
-- columns: a
-- row: ["{\"a\":1}"]
-- end-expected
SELECT to_json(ARRAY['{"a":1}'::text])::text AS a;
-- A document handed to the text search functions is read as a document
-- begin-expected
-- columns: a
-- row: 'cat':1 'zz':3
-- end-expected
SELECT to_tsvector('english', '{"b":"cat","a":"zz","c":1}'::json)::text AS a;
-- begin-expected
-- columns: a
-- row: 'cat':3 'zz':1
-- end-expected
SELECT to_tsvector('english', '{"b":"cat","a":"zz","c":1}'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: '1':10 'b':1 'c':8 'cat':3 'zz':6
-- end-expected
SELECT json_to_tsvector('english', '{"b":"cat","a":"zz","c":1}'::json, '["all"]')::text AS a;
-- begin-expected
-- columns: a
-- row: '1':10 'b':4 'c':8 'cat':6 'zz':2
-- end-expected
SELECT jsonb_to_tsvector('english', '{"b":"cat","a":"zz","c":1}'::jsonb, '["all"]')::text AS a;
-- begin-expected
-- columns: a
-- row: 'bb':1 'cat':3 'zz':6
-- end-expected
SELECT json_to_tsvector('english', '{"bb":"cat","a":"zz"}'::json, '["all"]')::text AS a;
-- begin-expected
-- columns: a
-- row: 'bb':4 'cat':6 'zz':2
-- end-expected
SELECT jsonb_to_tsvector('english', '{"bb":"cat","a":"zz"}'::jsonb, '["all"]')::text AS a;
-- begin-expected
-- columns: a
-- row: 'cat':2
-- end-expected
SELECT json_to_tsvector('english', '{"a":"cat"}'::json, '["all"]')::text AS a;
-- begin-expected
-- columns: a
-- row: 'cat':3
-- end-expected
SELECT json_to_tsvector('english', '["the","a","cat"]'::json, '["all"]')::text AS a;
-- begin-expected
-- columns: a
-- row: 'bb':1 'cat':2 'zz':4
-- end-expected
SELECT to_tsvector('english', 'bb cat a zz')::text AS a;
-- and what it hands back is a document too
-- begin-expected
-- columns: a
-- row: {"b":"<b>cat</b>","a":"zz"}
-- end-expected
SELECT ts_headline('english', '{"b":"cat","a":"zz"}'::json, to_tsquery('english','cat'))::text AS a;
-- begin-expected
-- columns: a
-- row: {"a": "zz", "b": "<b>cat</b>"}
-- end-expected
SELECT ts_headline('english', '{"b":"cat","a":"zz"}'::jsonb, to_tsquery('english','cat'))::text AS a;
-- begin-expected
-- columns: a
-- row: json
-- end-expected
SELECT pg_typeof(ts_headline('english', '{"b":"cat"}'::json, to_tsquery('english','cat')))::text AS a;
-- begin-expected
-- columns: a
-- row: jsonb
-- end-expected
SELECT pg_typeof(ts_headline('english', '{"b":"cat"}'::jsonb, to_tsquery('english','cat')))::text AS a;
-- begin-expected
-- columns: a
-- row: text
-- end-expected
SELECT pg_typeof(ts_headline('english', 'cat', to_tsquery('english','cat')))::text AS a;
DROP TABLE IF EXISTS jt_docs CASCADE;
