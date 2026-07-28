-- ============================================================================
-- Feature Comparison: JSON container-type errors, string escapes, jsonpath
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A JSON function handed the wrong container answers with a plausible value
-- instead of raising: the length of an object came out as 0, the keys of an
-- array as NULL, and setting a path in a scalar returned the scalar unchanged.
-- PostgreSQL names the container it wanted and stops.
--
-- Also covered: the string escapes jsonb cannot represent (a NUL, half a
-- surrogate pair) against the ones it can, and arithmetic written around a
-- jsonpath together with the error suppression @?, @@ and silent perform.
-- ============================================================================

DROP TABLE IF EXISTS jce_t CASCADE;
CREATE TABLE jce_t (id int, j jsonb);
INSERT INTO jce_t VALUES (1, '{"b":1,"aa":2}'), (2, '"scalar"'), (3, '[1,2]');

-- ============================================================================
-- 1. Length, keys and elements of the wrong container
-- ============================================================================
SELECT jsonb_array_length('{"a":1}'::jsonb) AS a;
SELECT jsonb_array_length('3'::jsonb) AS a;
SELECT jsonb_array_length('"x"'::jsonb) AS a;
SELECT jsonb_array_length('null'::jsonb) AS a;
SELECT json_array_length('{"a":1}'::json) AS a;
SELECT json_array_length('3'::json) AS a;
SELECT jsonb_object_keys('[1,2]'::jsonb) AS a;
SELECT jsonb_object_keys('3'::jsonb) AS a;
SELECT json_object_keys('[1,2]'::json) AS a;
SELECT json_object_keys('3'::json) AS a;
SELECT jsonb_array_elements('{"a":1}'::jsonb) AS a;
SELECT jsonb_array_elements('3'::jsonb) AS a;
SELECT jsonb_array_elements_text('{"a":1}'::jsonb) AS a;
SELECT jsonb_array_elements_text('3'::jsonb) AS a;
SELECT json_array_elements('{"a":1}'::json) AS a;
SELECT json_array_elements('3'::json) AS a;
SELECT json_array_elements_text('{"a":1}'::json) AS a;
SELECT json_array_elements_text('3'::json) AS a;
SELECT * FROM jsonb_array_elements('{"a":1}'::jsonb);
SELECT * FROM json_array_elements('3'::json);
SELECT * FROM jsonb_object_keys('[1]'::jsonb);
SELECT * FROM jsonb_each('[1,2]'::jsonb);
SELECT * FROM jsonb_each('"x"'::jsonb);
SELECT * FROM jsonb_each_text('3'::jsonb);
SELECT * FROM json_each('[1,2]'::json);
SELECT * FROM json_each('3'::json);
SELECT * FROM json_each_text('[1,2]'::json);
SELECT * FROM json_each_text('3'::json);

-- the same shapes that are right keep working
SELECT jsonb_array_length('[1,2,3]'::jsonb) AS a;
SELECT jsonb_array_length('[]'::jsonb) AS a;
SELECT jsonb_array_length('["a,b"]'::jsonb) AS a;
SELECT jsonb_array_length('[1,[2,3],4]'::jsonb) AS a;
SELECT json_array_length('[1,2]'::json) AS a;
SELECT jsonb_object_keys('{"a":1,"b":2}'::jsonb) AS a;
SELECT * FROM jsonb_object_keys('{"bb":1,"a":2}'::jsonb);
SELECT * FROM json_object_keys('{"a":1,"b":2}'::json);
SELECT * FROM jsonb_array_elements('[{"a":1},2]'::jsonb);
SELECT * FROM jsonb_array_elements_text('[null,"a",1]'::jsonb);
SELECT * FROM jsonb_each('{"a":1}'::jsonb);
SELECT * FROM jsonb_each_text('{"a":"x","b":2}'::jsonb);
SELECT jsonb_array_length(NULL::jsonb) AS a;
SELECT * FROM jsonb_array_elements(NULL::jsonb);
SELECT * FROM jsonb_object_keys(NULL::jsonb);
SELECT * FROM jsonb_each(NULL::jsonb);
SELECT jsonb_array_length(j) AS a FROM jce_t WHERE id = 3;
SELECT jsonb_object_keys(j) AS a FROM jce_t WHERE id = 1;

-- ============================================================================
-- 2. Deleting from the wrong container
-- ============================================================================
SELECT '{"a":1}'::jsonb - 0 AS a;
SELECT '{"a":1}'::jsonb - 1 AS a;
SELECT '"x"'::jsonb - 'a' AS a;
SELECT '3'::jsonb - 'a' AS a;
SELECT 'true'::jsonb - 'a' AS a;
SELECT '3'::jsonb - '{a}'::text[] AS a;
SELECT '3'::jsonb #- '{a}' AS a;
SELECT '[1,2]'::jsonb #- '{a}' AS a;
SELECT '{"a":[1]}'::jsonb #- '{a,x}' AS a;
SELECT j - 'x' AS a FROM jce_t WHERE id = 2;

-- deletions that do apply keep working
SELECT '[1,2]'::jsonb - 0 AS a;
SELECT '[1,2]'::jsonb - 5 AS a;
SELECT '[1,2]'::jsonb - (-1) AS a;
SELECT '{"a":1}'::jsonb - 'a' AS a;
SELECT '["a","b"]'::jsonb - 'a' AS a;
SELECT '{"a":1,"b":2}'::jsonb - '{a,b}'::text[] AS a;
SELECT '["a","b"]'::jsonb - '{a}'::text[] AS a;
SELECT '[1,2]'::jsonb - '{0}'::text[] AS a;
SELECT '[1,2,3]'::jsonb #- '{1}' AS a;
SELECT '{"a":{"b":1}}'::jsonb #- '{a,b}' AS a;
SELECT '{"a":1}'::jsonb #- '{a,0}' AS a;
SELECT '{"a":"s"}'::jsonb #- '{a,b}' AS a;
SELECT '{"a":1}'::jsonb #- '{}' AS a;

-- ============================================================================
-- 3. Setting a path in a scalar, and a path step an array cannot use
-- ============================================================================
SELECT jsonb_set('1'::jsonb, '{a}', '2') AS a;
SELECT jsonb_set('"s"'::jsonb, '{a}', '2') AS a;
SELECT jsonb_set('null'::jsonb, '{a}', '2') AS a;
SELECT jsonb_set('[1,2]'::jsonb, '{a}', '9') AS a;
SELECT jsonb_set('{"a":[1,2]}'::jsonb, '{a,x}', '9') AS a;
SELECT jsonb_set('[[1]]'::jsonb, '{0,x}', '9') AS a;
SELECT jsonb_insert('1'::jsonb, '{a}', '2') AS a;
SELECT jsonb_insert('"x"'::jsonb, '{0}', '9') AS a;
SELECT jsonb_insert('[1,2]'::jsonb, '{x}', '9') AS a;
SELECT jsonb_insert('{"a":[1]}'::jsonb, '{a,x}', '9') AS a;

-- the paths that do apply keep working
SELECT jsonb_set('[1,2]'::jsonb, '{0}', '9') AS a;
SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '9') AS a;
SELECT jsonb_set('{"a":[1,2]}'::jsonb, '{a,1}', '9') AS a;
SELECT jsonb_set('{"a":{"b":[1,2]}}'::jsonb, '{a,b,0}', '"z"') AS a;
SELECT jsonb_set('{"a":1}'::jsonb, '{}', '9') AS a;
SELECT jsonb_set('{"a":1}'::jsonb, '{b}', '2', false) AS a;
SELECT jsonb_set('{"a":1}'::jsonb, '{b}', '2', true) AS a;
SELECT jsonb_set('{"a":1}'::jsonb, '{b,c}', '9') AS a;
SELECT jsonb_set('{"a":"s"}'::jsonb, '{a,b}', '9') AS a;
SELECT jsonb_set('{"a":[1]}'::jsonb, '{a,5}', '9') AS a;
SELECT jsonb_insert('[1,2]'::jsonb, '{1}', '9') AS a;
SELECT jsonb_insert('[1,2]'::jsonb, '{1}', '9', true) AS a;
SELECT jsonb_insert('{"a":1}'::jsonb, '{b}', '9') AS a;
SELECT jsonb_insert('{"a":1}'::jsonb, '{a}', '9') AS a;
SELECT jsonb_insert('{"a":[1,2]}'::jsonb, '{a,0}', '9') AS a;
SELECT jsonb_set(j, '{b}', '9') AS a FROM jce_t WHERE id = 1;

-- ============================================================================
-- 4. Building an object from arguments that do not pair up
-- ============================================================================
SELECT json_build_object('a') AS a;
SELECT jsonb_build_object('a') AS a;
SELECT json_build_object(NULL, 1) AS a;
SELECT jsonb_build_object(NULL, 1) AS a;
SELECT jsonb_build_object('a', 1, NULL, 2) AS a;
SELECT json_object('{a}') AS a;
SELECT jsonb_object('{a}') AS a;

-- and the ones that do pair up
SELECT json_build_object() AS a;
SELECT json_build_object('a', 1) AS a;
SELECT json_build_object('a', 1, 'b', 2) AS a;
SELECT json_build_object('a', 1, 'b', NULL) AS a;
SELECT jsonb_build_object('a', 1) AS a;
SELECT jsonb_build_object('a', 1, 'b', NULL) AS a;
SELECT json_build_array(1, 'a') AS a;
SELECT json_object('{a,1,b,2}') AS a;
SELECT json_object('{a,b}', '{1,2}') AS a;
SELECT jsonb_object('{a,1,b,2}') AS a;
SELECT jsonb_object('{a,b}', '{1,2}') AS a;
SELECT JSON_OBJECT('a' : 1) AS a;
SELECT JSON_OBJECT('a' VALUE 1) AS a;
SELECT json_object_agg(k, v) AS a FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) t;
SELECT jsonb_object_agg(k, v) AS a FROM (SELECT 'a' AS k, 1 AS v) t;

-- ============================================================================
-- 5. String escapes jsonb cannot represent
-- ============================================================================
SELECT '"\u0000"'::jsonb AS a;
SELECT '{"a": "\u0000"}'::jsonb AS a;
SELECT '"\ud834"'::jsonb AS a;
SELECT '"\udd1e"'::jsonb AS a;
SELECT '"\ud834A"'::jsonb AS a;
SELECT '"\ud834\u0041"'::jsonb AS a;
SELECT '"\u"'::jsonb AS a;
SELECT '"\u12"'::jsonb AS a;
SELECT '"\q"'::jsonb AS a;
INSERT INTO jce_t VALUES (4, '"\u0000"');
INSERT INTO jce_t VALUES (5, '"\ud834"');

-- json keeps the text it was given, and only jsonb has to decode it
SELECT '"\u0000"'::json AS a;
SELECT '"\ud834"'::json AS a;
SELECT '"\u0041"'::json AS a;
SELECT '"\q"'::json AS a;
SELECT '"\u12"'::json AS a;

-- escapes jsonb can represent survive, and come back in canonical form
SELECT '"\u0041"'::jsonb AS a;
SELECT '"\ud834\udd1e"'::jsonb AS a;
SELECT '"a\/b"'::jsonb AS a;
SELECT '"a\nb"'::jsonb AS a;
SELECT '"\t"'::jsonb AS a;
SELECT '"a\\b"'::jsonb AS a;
SELECT '"a\"b"'::jsonb AS a;
SELECT '["a\\"]'::jsonb AS a;
SELECT '["a\\"]'::jsonb -> 0 AS a;
SELECT '["a\\"]'::jsonb ->> 0 AS a;
SELECT '{"a\\":1}'::jsonb AS a;
SELECT '{"a\\b":1}'::jsonb AS a;
SELECT '{"a\\b":1}'::jsonb -> 'a\b' AS a;
SELECT jsonb_pretty('["a\\"]'::jsonb) AS a;
SELECT jsonb_pretty('"a\\b"'::jsonb) AS a;
SELECT jsonb_pretty('{"a":1,"b":[1,2]}'::jsonb) AS a;
SELECT to_json('a\b'::text) AS a;
SELECT to_jsonb('a\b'::text) AS a;
SELECT to_jsonb('a"b'::text) AS a;
SELECT '{"a":"b\\"}'::jsonb || '{"c":1}'::jsonb AS a;

-- and a raw control character is still not a JSON string
SELECT '"a
b"'::jsonb AS a;

-- ============================================================================
-- 6. Arithmetic written around a jsonpath
-- ============================================================================
SELECT jsonb_path_query('[2]', '$[0] + 3') AS a;
SELECT jsonb_path_query('[2]', '$[0] - 3') AS a;
SELECT jsonb_path_query('[2]', '$[0] * 3') AS a;
SELECT jsonb_path_query('[6]', '$[0] / 3') AS a;
SELECT jsonb_path_query('[7]', '$[0] % 3') AS a;
SELECT jsonb_path_query('[-10]', '$[0] % 4') AS a;
SELECT jsonb_path_query('[2]', '7 - $[0]') AS a;
SELECT jsonb_path_query('[4]', '2 * $[0]') AS a;
SELECT jsonb_path_query('[2]', '10 / $[0]') AS a;
SELECT jsonb_path_query('[2]', '- $[0]') AS a;
SELECT jsonb_path_query('[2]', '+ $[0]') AS a;
SELECT jsonb_path_query('[1]', '-1') AS a;
SELECT jsonb_path_query('[1]', '1 + 2') AS a;
SELECT jsonb_path_query('[1]', '(1 + 2)') AS a;
SELECT jsonb_path_query('[1]', '(1 + 2) * 3') AS a;
SELECT jsonb_path_query('[1]', '$[0] + 1 + 2') AS a;
SELECT jsonb_path_query('[1]', '$[0] + $[0]') AS a;
SELECT jsonb_path_query('[1]', '$[0] - -1') AS a;
SELECT jsonb_path_query('[1]', '2 - - 1') AS a;
SELECT jsonb_path_query('[1]', '$[0] ++ 1') AS a;
SELECT jsonb_path_query('[1]', '$[0] + $[0] * 2') AS a;
SELECT jsonb_path_query('[2]', '$[0] * $[0] * $[0]') AS a;
SELECT jsonb_path_query('[1.5]', '$[0] + 1') AS a;
SELECT jsonb_path_query('[1]', '$[0] + 1.50') AS a;
SELECT jsonb_path_query('[0.1]', '$[0] + 0.2') AS a;
SELECT jsonb_path_query('[7]', '$[0] % 2.5') AS a;
SELECT jsonb_path_query('[1e10]', '$[0] * 2') AS a;
SELECT jsonb_path_query('[1]', '$[0] / 3') AS a;
SELECT jsonb_path_query('[100]', '$[0] / 7') AS a;
SELECT jsonb_path_query('[1]', '$[0] / 30000') AS a;
SELECT jsonb_path_query('[1.5]', '$[0] / 3') AS a;
SELECT jsonb_path_query('[10000]', '$[0] / 3') AS a;
SELECT jsonb_path_query('{"a":2}', '$.a + $.a') AS a;
SELECT jsonb_path_query('{"x":2}', '2 * $.x + 1') AS a;
SELECT jsonb_path_query('{"x":2}', '(2 + $.x) * 2') AS a;
SELECT jsonb_path_query('{"x":2}', '- $.x + 1') AS a;
SELECT jsonb_path_query('{"x":[2]}', '$.x[0] + 1') AS a;
SELECT jsonb_path_query('{"x":2}', '$.x + $y', '{"y": 3}') AS a;
SELECT jsonb_path_query_array('{"x":[2,3,4]}', '+ $.x') AS a;
SELECT jsonb_path_query_array('{"x":[2,3,4]}', '- $.x') AS a;
SELECT jsonb_path_query_array('{"x":[2,3,4]}', '- $.x[*]') AS a;
SELECT jsonb_path_query('[1,2]', '- $') AS a;
SELECT jsonb_path_query('[[1,2]]', '- $[0]') AS a;
SELECT jsonb_path_query_first('[1,2]', '$[*] + 0') AS a;
SELECT jsonb_path_exists('[1]', '$[0] + 1') AS a;

-- an operand that is not one numeric value, and a divisor of zero
SELECT jsonb_path_query('{"x":[2,3,4]}', '$.x + 1') AS a;
SELECT jsonb_path_query_array('[1,2,3]', '$[*] + 1') AS a;
SELECT jsonb_path_query_array('[1,2]', '$[*] * 2') AS a;
SELECT jsonb_path_query('[1,2,3]', '$[*] ? (@ > 1) + 1') AS a;
SELECT jsonb_path_query('"a"', '$ + 1') AS a;
SELECT jsonb_path_query('1', '$ + "a"') AS a;
SELECT jsonb_path_query('["a"]', '- $') AS a;
SELECT jsonb_path_query('{"a":1}', '- $') AS a;
SELECT jsonb_path_query('{"a":true}', '- $.a') AS a;
SELECT jsonb_path_query('[2]', '$[0] / 0') AS a;
SELECT jsonb_path_query('[1]', '$[0] + ') AS a;

-- paths with no arithmetic in them are untouched
SELECT jsonb_path_query('{"a":1}', '$.*') AS a;
SELECT jsonb_path_query('{"a":[1,2,3]}', '$.a[*]') AS a;
SELECT jsonb_path_query('{"a":{"b":2}}', '$.a.b') AS a;
SELECT jsonb_path_query('[{"x":1},{"x":2}]', '$[*].x') AS a;
SELECT jsonb_path_query_array('{"a":[1,2,3]}', '$.a[*] ? (@ > 1)') AS a;
SELECT jsonb_path_query_array('[1,2]', '$[*].a') AS a;
SELECT jsonb_path_query_first('{"a":[5,6]}', '$.a[*]') AS a;
SELECT jsonb_path_exists('{"a":1}', '$.a') AS a;
SELECT jsonb_path_exists('{"a":1}', '$.b') AS a;
SELECT jsonb_path_exists('[1,2]', '$[*] ? (@ == 2)') AS a;
SELECT jsonb_path_match('{"a":1}', '$.a == 1') AS a;

-- ============================================================================
-- 7. Error suppression: @?, @@ and the silent argument
-- ============================================================================
SELECT '{"a":[1,2,3]}'::jsonb @? 'strict $.b' AS a;
SELECT '{"a":[1,2,3]}'::jsonb @@ 'strict $.b == 1' AS a;
SELECT '1'::jsonb @? 'strict $[*]' AS a;
SELECT '1'::jsonb @@ 'strict $[*] == 1' AS a;
SELECT '[1,2]'::jsonb @? '$[*] + 1' AS a;
SELECT '[2]'::jsonb @? '$[0] / 0' AS a;
SELECT jsonb_path_exists('{"a":[1,2,3]}', 'strict $.b', '{}', true) AS a;
SELECT jsonb_path_exists('{}', 'strict $.a', '{}', true) AS a;
SELECT jsonb_path_exists('{"a":1}', 'strict $.a.b', '{}', true) AS a;
SELECT jsonb_path_exists('1', 'strict $[*]', '{}', true) AS a;
SELECT jsonb_path_query('{"a":[1,2,3]}', 'strict $.b', '{}', true) AS a;
SELECT jsonb_path_query('{"x":[2,3,4]}', '$.x + 1', '{}', true) AS a;
SELECT jsonb_path_query('[2]', '$[0] / 0', '{}', true) AS a;
SELECT jsonb_path_query('[1,2]', '$[*] + 1', '{}', true) AS a;
SELECT jsonb_path_query('1', '$ + "a"', '{}', true) AS a;
SELECT jsonb_path_query_array('{"a":[1,2,3]}', 'strict $.b', '{}', true) AS a;
SELECT jsonb_path_query_array('{}', 'strict $.a', '{}', true) AS a;
SELECT jsonb_path_query_first('{"a":[1,2,3]}', 'strict $.b', '{}', true) AS a;
SELECT jsonb_path_match('{"a":[1,2,3]}', 'strict $.b == 1', '{}', true) AS a;
SELECT jsonb_path_match('{}', 'strict $.a == 1', '{}', true) AS a;

-- without suppression the same paths still raise, and @? still answers
SELECT jsonb_path_exists('{"a":[1,2,3]}', 'strict $.b') AS a;
SELECT jsonb_path_exists('{"a":[1,2,3]}', 'strict $.b', '{}', false) AS a;
SELECT jsonb_path_query('{"a":[1,2]}', 'strict $.a[*]') AS a;
SELECT '{"a":[1,2,3]}'::jsonb @? '$.a' AS a;
SELECT '{"a":[1,2,3]}'::jsonb @? 'lax $.b' AS a;
SELECT '{"a":1}'::jsonb @? '$.a ? (@ > 0)' AS a;
SELECT '[1]'::jsonb @? 'strict $[*] ? (@ > 0)' AS a;
SELECT '{"a":1}'::jsonb @@ '$.a == 1' AS a;
SELECT '{"a":1}'::jsonb @@ '$.a == 2' AS a;
SELECT jsonb_path_query('{"a":1}', '$.a', '{}', true) AS a;

DROP TABLE jce_t;
