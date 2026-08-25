-- ============================================================================
-- -- A JSON document is the value it spells, not the text it prints as.
-- --
-- -- Whether a text is JSON at all is answered by reading it, and JSON's numbers are JSON's: a
-- -- heuristic that handed them to a Java double parse read 1., .5 and 1d, which JSON does not
-- -- write. Once read, a jsonb is the document rather than the text that spells it, so 1 and 1.0
-- -- are one value wherever rows are gathered into groups. json is the other way round: it is kept
-- -- as it was written and PostgreSQL gives it no equality at all, so those same clauses have
-- -- nothing to gather json values by. And what tells a document from a string is its type, since
-- -- the two are the same characters -- which is what decides how a document put inside a larger
-- -- one is written.
--
-- ============================================================================

-- setup
CREATE TABLE jdoc (id int, j jsonb, g json);
INSERT INTO jdoc VALUES (1, '1', '1'), (2, '1.0', '1.0'), (3, '{"a":1,"b":2}', '{"a":1}');

-- A number is written the way JSON writes one
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '1.'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '.5'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '1d'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '01'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '+1'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '1e'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '0x10'::jsonb;
-- begin-expected
-- columns: jsonb
-- row: 1000
-- end-expected
SELECT '1e3'::jsonb;
-- begin-expected
-- columns: jsonb
-- row: -0.005
-- end-expected
SELECT '-0.5e-2'::jsonb;
-- begin-expected
-- columns: jsonb
-- row: 0
-- end-expected
SELECT '-0'::jsonb;

-- A document ends where it ends
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '1 2'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '{} {}'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '[1,]'::jsonb;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '{"a":1,}'::jsonb;

-- Whether a text is JSON is answered by the reader
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '{"a":1}'::text IS JSON;
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT '1.'::text IS JSON;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '1'::text IS JSON SCALAR;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '[1]'::text IS JSON ARRAY;
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT '{"a":1,"a":2}'::text IS JSON WITH UNIQUE KEYS;
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT '{"a":{"b":1,"b":2}}'::text IS JSON WITH UNIQUE KEYS;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '{"a":{"b":1},"b":2}'::text IS JSON WITH UNIQUE KEYS;

-- A number is the number it is, however it was written
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '1'::jsonb = '1.0'::jsonb;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '{"a":1}'::jsonb = '{"a":1.0}'::jsonb;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '[1]'::jsonb = '[1.00]'::jsonb;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '{"a":1,"b":2}'::jsonb = '{"b":2,"a":1}'::jsonb;

-- Rows are gathered by the document they spell
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(DISTINCT j) FROM jdoc;
-- begin-expected
-- columns: j
-- row: 1
-- row: {"a": 1, "b": 2}
-- end-expected
SELECT j FROM jdoc GROUP BY j ORDER BY j::text;
-- begin-expected
-- columns: j
-- row: 1
-- row: {"a": 1, "b": 2}
-- end-expected
SELECT DISTINCT j FROM jdoc ORDER BY 1;
-- begin-expected
-- columns: id
-- row: 1
-- row: 3
-- end-expected
SELECT DISTINCT ON (j) id FROM jdoc ORDER BY j, id;
-- begin-expected
-- columns: count
-- row: 2
-- row: 2
-- row: 1
-- end-expected
SELECT count(*) OVER (PARTITION BY j) FROM jdoc ORDER BY id;
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT j FROM jdoc UNION SELECT j FROM jdoc) s;
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT j FROM jdoc WHERE id IN (1,2) INTERSECT SELECT j FROM jdoc WHERE id = 2) s;
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT j FROM jdoc EXCEPT SELECT j FROM jdoc WHERE id = 1) s;

-- json values cannot be gathered into groups
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT DISTINCT g FROM jdoc;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT g FROM jdoc GROUP BY g;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT g FROM jdoc UNION SELECT g FROM jdoc;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT count(DISTINCT g) FROM jdoc;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT count(*) OVER (PARTITION BY g) FROM jdoc;

-- The operators that read a document are jsonb's
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '{"a":1}'::json ? 'a';
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '{"a":1}'::json #- '{a}';
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '{"a":1}'::json @? '$.a';
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '{"a":1}'::json @> '{}'::json;
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT '{"a":1}'::json #> '{a}';
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT '{"a":1}'::json -> 'a';
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT '{"a":1}'::json ->> 'a';

-- A document is written where it stands
-- begin-expected
-- columns: to_jsonb
-- row: 1
-- end-expected
SELECT to_jsonb(j) FROM jdoc WHERE id = 1;
-- begin-expected
-- columns: jsonb_agg
-- row: [1, {"a": 1, "b": 2}]
-- end-expected
SELECT jsonb_agg(j ORDER BY id) FROM jdoc WHERE id IN (1,3);
-- begin-expected
-- columns: jsonb_build_array
-- row: [1, 1]
-- end-expected
SELECT jsonb_build_array(j, 1) FROM jdoc WHERE id = 1;
-- begin-expected
-- columns: json_agg
-- row: [1, {"a":1}]
-- end-expected
SELECT json_agg(g ORDER BY id) FROM jdoc WHERE id IN (1,3);
-- begin-expected
-- columns: to_jsonb
-- row: [1, {"a": 1, "b": 2}]
-- end-expected
SELECT to_jsonb(array_agg(j ORDER BY id)) FROM jdoc WHERE id IN (1,3);
-- begin-expected
-- columns: jsonb_agg
-- row: [1]
-- end-expected
SELECT jsonb_agg(DISTINCT j) FROM jdoc WHERE id IN (1,2);

-- What must be escaped is escaped, in keys as in values
-- begin-expected
-- columns: to_json
-- row: ["a\"b","a\tb"]
-- end-expected
SELECT to_json(ARRAY['a"b', E'a\tb']);
-- begin-expected
-- columns: jsonb_build_object
-- row: {"k\tey": "v\nal"}
-- end-expected
SELECT jsonb_build_object(E'k\tey', E'v\nal');

-- The numbers JSON cannot write are written as text
-- begin-expected
-- columns: to_json
-- row: "Infinity"
-- end-expected
SELECT to_json('Infinity'::float8);
-- begin-expected
-- columns: to_jsonb
-- row: "NaN"
-- end-expected
SELECT to_jsonb('NaN'::float8);
-- begin-expected
-- columns: to_json
-- row: "2020-01-02T03:04:05"
-- end-expected
SELECT to_json('2020-01-02 03:04:05'::timestamp);

-- A null argument is answered with null, but a key cannot be null
-- begin-expected
-- columns: jsonb_set
-- row: NULL
-- end-expected
SELECT jsonb_set('{"a":1}', NULL, '2');
-- begin-expected
-- columns: jsonb_insert
-- row: NULL
-- end-expected
SELECT jsonb_insert('{"a":1}', NULL, '2');
-- begin-expected
-- columns: json_typeof
-- row: NULL
-- end-expected
SELECT json_typeof(NULL::json);
-- begin-expected-error
-- sqlstate: 22004
-- end-expected-error
SELECT jsonb_object(ARRAY[NULL, 'b']);
-- begin-expected
-- columns: jsonb_object
-- row: {"a": "1", "b": "2"}
-- end-expected
SELECT jsonb_object(ARRAY['a','b'], ARRAY['1','2']);

-- jsonb prints the document back; json prints the text it was given
-- begin-expected
-- columns: text
-- row: {"a": 1}
-- end-expected
SELECT ('{"a":1}'::jsonb)::text;
-- begin-expected
-- columns: text
-- row: {"a":1}
-- end-expected
SELECT ('{"a":1}'::json)::text;


-- teardown
DROP TABLE jdoc;
