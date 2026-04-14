-- ============================================================================
-- Feature Comparison: SQL/JSON Standard Functions (PG 16+, A1)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- SQL/JSON functions: IS JSON, JSON_EXISTS, JSON_VALUE, JSON_QUERY,
-- JSON_TABLE, JSON_SCALAR, JSON_SERIALIZE, JSON_ARRAYAGG, JSON_OBJECTAGG,
-- JSON_ARRAY, JSON_OBJECT constructors.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS sj_test CASCADE;
CREATE SCHEMA sj_test;
SET search_path = sj_test, public;

CREATE TABLE sj_data (id integer PRIMARY KEY, doc jsonb);
INSERT INTO sj_data VALUES
  (1, '{"name": "Alice", "age": 30, "tags": ["admin", "user"]}'),
  (2, '{"name": "Bob", "age": 25, "tags": ["user"]}'),
  (3, '{"name": "Charlie", "age": 35, "active": true}'),
  (4, NULL);

-- ============================================================================
-- IS JSON predicate
-- ============================================================================

-- ============================================================================
-- 1. IS JSON — valid JSON
-- ============================================================================

-- begin-expected
-- columns: r1, r2, r3
-- row: true, true, false
-- end-expected
SELECT
  '{"a":1}' IS JSON AS r1,
  '[1,2,3]' IS JSON AS r2,
  'not json' IS JSON AS r3;

-- ============================================================================
-- 2. IS JSON VALUE / OBJECT / ARRAY / SCALAR
-- ============================================================================

-- begin-expected
-- columns: is_obj, is_arr, is_scalar, is_value
-- row: true, false, false, true
-- end-expected
SELECT
  '{"a":1}' IS JSON OBJECT AS is_obj,
  '{"a":1}' IS JSON ARRAY AS is_arr,
  '{"a":1}' IS JSON SCALAR AS is_scalar,
  '{"a":1}' IS JSON VALUE AS is_value;

-- begin-expected
-- columns: is_obj, is_arr, is_scalar
-- row: false, true, false
-- end-expected
SELECT
  '[1,2]' IS JSON OBJECT AS is_obj,
  '[1,2]' IS JSON ARRAY AS is_arr,
  '[1,2]' IS JSON SCALAR AS is_scalar;

-- begin-expected
-- columns: is_scalar
-- row: true
-- end-expected
SELECT '"hello"' IS JSON SCALAR AS is_scalar;

-- ============================================================================
-- 3. IS NOT JSON
-- ============================================================================

-- begin-expected
-- columns: r1, r2
-- row: false, true
-- end-expected
SELECT '{"a":1}' IS NOT JSON AS r1, 'xyz' IS NOT JSON AS r2;

-- ============================================================================
-- 4. IS JSON WITH UNIQUE KEYS
-- ============================================================================

-- begin-expected
-- columns: unique_keys, dup_keys
-- row: true, false
-- end-expected
SELECT
  '{"a":1,"b":2}' IS JSON OBJECT WITH UNIQUE KEYS AS unique_keys,
  '{"a":1,"a":2}' IS JSON OBJECT WITH UNIQUE KEYS AS dup_keys;

-- ============================================================================
-- 5. IS JSON on NULL
-- ============================================================================

-- begin-expected
-- columns: is_null_json
-- row: NULL
-- end-expected
SELECT NULL IS JSON AS is_null_json;

-- ============================================================================
-- JSON_EXISTS
-- ============================================================================

-- ============================================================================
-- 6. JSON_EXISTS — basic path
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_EXISTS('{"a":1,"b":2}', '$.a') AS result;

-- begin-expected
-- columns: result
-- row: false
-- end-expected
SELECT JSON_EXISTS('{"a":1}', '$.c') AS result;

-- ============================================================================
-- 7. JSON_EXISTS with nested path
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_EXISTS('{"a":{"b":{"c":1}}}', '$.a.b.c') AS result;

-- ============================================================================
-- 8. JSON_EXISTS with array element
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_EXISTS('{"tags":["a","b","c"]}', '$.tags[1]') AS result;

-- ============================================================================
-- 9. JSON_EXISTS with PASSING
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_EXISTS('{"items":[10,20,30]}', '$.items[*] ? (@ > $x)' PASSING 15 AS x) AS result;

-- ============================================================================
-- 10. JSON_EXISTS ON ERROR
-- ============================================================================

-- begin-expected-error
-- message-like: invalid input syntax
-- end-expected-error
SELECT JSON_EXISTS('not json', '$.a' TRUE ON ERROR) AS result;

-- begin-expected-error
-- message-like: invalid input syntax
-- end-expected-error
SELECT JSON_EXISTS('not json', '$.a' FALSE ON ERROR) AS result;

-- ============================================================================
-- JSON_VALUE
-- ============================================================================

-- ============================================================================
-- 11. JSON_VALUE — extract scalar
-- ============================================================================

-- begin-expected
-- columns: result
-- row: Alice
-- end-expected
SELECT JSON_VALUE('{"name":"Alice","age":30}', '$.name') AS result;

-- begin-expected
-- columns: result
-- row: 30
-- end-expected
SELECT JSON_VALUE('{"name":"Alice","age":30}', '$.age') AS result;

-- ============================================================================
-- 12. JSON_VALUE — missing path returns NULL
-- ============================================================================

-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT JSON_VALUE('{"a":1}', '$.missing') IS NULL AS is_null;

-- ============================================================================
-- 13. JSON_VALUE RETURNING type
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 42
-- end-expected
SELECT JSON_VALUE('{"x":42}', '$.x' RETURNING integer) AS result;

-- ============================================================================
-- 14. JSON_VALUE DEFAULT ON EMPTY
-- ============================================================================

-- begin-expected
-- columns: result
-- row: N/A
-- end-expected
SELECT JSON_VALUE('{"a":1}', '$.missing' DEFAULT 'N/A' ON EMPTY) AS result;

-- ============================================================================
-- 15. JSON_VALUE DEFAULT ON ERROR
-- ============================================================================

-- begin-expected-error
-- message-like: invalid input syntax
-- end-expected-error
SELECT JSON_VALUE('not json', '$.a' DEFAULT 'error-fallback' ON ERROR) AS result;

-- ============================================================================
-- 16. JSON_VALUE ERROR ON EMPTY
-- ============================================================================

-- begin-expected-error
-- message-like: no SQL/JSON item
-- end-expected-error
SELECT JSON_VALUE('{"a":1}', '$.missing' ERROR ON EMPTY);

-- ============================================================================
-- 17. JSON_VALUE on table column
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 1, Alice
-- row: 2, Bob
-- row: 3, Charlie
-- end-expected
SELECT id, JSON_VALUE(doc::text, '$.name') AS name
FROM sj_data
WHERE doc IS NOT NULL
ORDER BY id;

-- ============================================================================
-- JSON_QUERY
-- ============================================================================

-- ============================================================================
-- 18. JSON_QUERY — extract object
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {"b": 1}
-- end-expected
SELECT JSON_QUERY('{"a":{"b":1}}', '$.a') AS result;

-- ============================================================================
-- 19. JSON_QUERY — extract array
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [1, 2, 3]
-- end-expected
SELECT JSON_QUERY('{"arr":[1,2,3]}', '$.arr') AS result;

-- ============================================================================
-- 20. JSON_QUERY WITH WRAPPER
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [1]
-- end-expected
SELECT JSON_QUERY('{"a":1}', '$.a' WITH WRAPPER) AS result;

-- ============================================================================
-- 21. JSON_QUERY WITHOUT WRAPPER (default)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [1, 2, 3]
-- end-expected
SELECT JSON_QUERY('{"arr":[1,2,3]}', '$.arr' WITHOUT WRAPPER) AS result;

-- ============================================================================
-- 22. JSON_QUERY KEEP QUOTES / OMIT QUOTES
-- ============================================================================

-- begin-expected
-- columns: result
-- row: "hello"
-- end-expected
SELECT JSON_QUERY('{"a":"hello"}', '$.a' KEEP QUOTES) AS result;

-- begin-expected
-- columns: result
-- row: NULL
-- end-expected
SELECT JSON_QUERY('{"a":"hello"}', '$.a' OMIT QUOTES) AS result;

-- ============================================================================
-- 23. JSON_QUERY EMPTY ARRAY ON EMPTY
-- ============================================================================

-- begin-expected
-- columns: result
-- row: []
-- end-expected
SELECT JSON_QUERY('{"a":1}', '$.missing' EMPTY ARRAY ON EMPTY) AS result;

-- ============================================================================
-- 24. JSON_QUERY EMPTY OBJECT ON EMPTY
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {}
-- end-expected
SELECT JSON_QUERY('{"a":1}', '$.missing' EMPTY OBJECT ON EMPTY) AS result;

-- ============================================================================
-- JSON_TABLE
-- ============================================================================

-- ============================================================================
-- 25. JSON_TABLE — basic COLUMNS
-- ============================================================================

-- begin-expected
-- columns: name, age
-- row: Alice, 30
-- row: Bob, 25
-- end-expected
SELECT jt.name, jt.age
FROM JSON_TABLE(
  '[{"name":"Alice","age":30},{"name":"Bob","age":25}]',
  '$[*]'
  COLUMNS (
    name text PATH '$.name',
    age integer PATH '$.age'
  )
) AS jt;

-- ============================================================================
-- 26. JSON_TABLE — ordinality column
-- ============================================================================

-- begin-expected
-- columns: rn, name
-- row: 1, Alice
-- row: 2, Bob
-- end-expected
SELECT jt.rn, jt.name
FROM JSON_TABLE(
  '[{"name":"Alice"},{"name":"Bob"}]',
  '$[*]'
  COLUMNS (
    rn FOR ORDINALITY,
    name text PATH '$.name'
  )
) AS jt;

-- ============================================================================
-- 27. JSON_TABLE — NESTED PATH
-- ============================================================================

-- begin-expected
-- columns: name, tag
-- row: Alice, admin
-- row: Alice, user
-- end-expected
SELECT jt.name, jt.tag
FROM JSON_TABLE(
  '[{"name":"Alice","tags":["admin","user"]}]',
  '$[*]'
  COLUMNS (
    name text PATH '$.name',
    NESTED PATH '$.tags[*]' COLUMNS (
      tag text PATH '$'
    )
  )
) AS jt;

-- ============================================================================
-- 28. JSON_TABLE — DEFAULT on missing
-- ============================================================================

-- begin-expected
-- columns: name, city
-- row: Alice, unknown
-- end-expected
SELECT jt.name, jt.city
FROM JSON_TABLE(
  '[{"name":"Alice"}]',
  '$[*]'
  COLUMNS (
    name text PATH '$.name',
    city text PATH '$.city' DEFAULT 'unknown' ON EMPTY
  )
) AS jt;

-- ============================================================================
-- 29. JSON_TABLE — ERROR ON ERROR
-- ============================================================================

-- begin-expected-error
-- message-like: JSON
-- end-expected-error
SELECT *
FROM JSON_TABLE(
  'not json',
  '$[*]'
  COLUMNS (name text PATH '$.name')
  ERROR ON ERROR
) AS jt;

-- ============================================================================
-- 30. JSON_TABLE in JOIN
-- ============================================================================

-- begin-expected
-- columns: id, tag
-- row: 1, admin
-- row: 1, user
-- end-expected
SELECT d.id, jt.tag
FROM sj_data d,
JSON_TABLE(
  d.doc::text,
  '$.tags[*]'
  COLUMNS (tag text PATH '$')
) AS jt
WHERE d.id = 1
ORDER BY jt.tag;

-- ============================================================================
-- JSON_SCALAR
-- ============================================================================

-- ============================================================================
-- 31. JSON_SCALAR — various types
-- ============================================================================

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: "hello", 42, true, null
-- end-expected
SELECT
  JSON_SCALAR('hello') AS r1,
  JSON_SCALAR(42) AS r2,
  JSON_SCALAR(true) AS r3,
  JSON_SCALAR(NULL) AS r4;

-- ============================================================================
-- JSON_SERIALIZE
-- ============================================================================

-- ============================================================================
-- 32. JSON_SERIALIZE — convert to text
-- ============================================================================

-- pg-bug: PG 18 JSON_SERIALIZE returns jsonb version byte (0x01) instead of serialized text — known PG bug, will be fixed upstream
-- begin-expected
-- columns: result
-- row: {"a": 1}
-- end-expected
SELECT JSON_SERIALIZE('{"a":1}'::jsonb) AS result;

-- ============================================================================
-- 33. JSON_SERIALIZE RETURNING
-- ============================================================================

-- begin-expected
-- columns: tp
-- row: text
-- end-expected
SELECT pg_typeof(JSON_SERIALIZE('{"a":1}'::jsonb RETURNING text)) AS tp;

-- ============================================================================
-- JSON_ARRAYAGG
-- ============================================================================

-- ============================================================================
-- 34. JSON_ARRAYAGG — basic
-- ============================================================================

-- begin-expected
-- columns: result
-- row: ["Alice", "Bob", "Charlie"]
-- end-expected
SELECT JSON_ARRAYAGG(JSON_VALUE(doc::text, '$.name') ORDER BY id) AS result
FROM sj_data
WHERE doc IS NOT NULL;

-- ============================================================================
-- 35. JSON_ARRAYAGG — NULL handling
-- ============================================================================

-- begin-expected
-- columns: with_nulls, without_nulls
-- row: [1, null, 3] | [1, 3]
-- end-expected
SELECT
  JSON_ARRAYAGG(val NULL ON NULL) AS with_nulls,
  JSON_ARRAYAGG(val ABSENT ON NULL) AS without_nulls
FROM (VALUES (1), (NULL::integer), (3)) AS t(val);

-- ============================================================================
-- 36. JSON_ARRAYAGG — ORDER BY
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [35, 30, 25]
-- end-expected
SELECT JSON_ARRAYAGG(
  (JSON_VALUE(doc::text, '$.age' RETURNING integer))
  ORDER BY JSON_VALUE(doc::text, '$.age' RETURNING integer) DESC
) AS result
FROM sj_data
WHERE doc IS NOT NULL;

-- ============================================================================
-- JSON_OBJECTAGG
-- ============================================================================

-- ============================================================================
-- 37. JSON_OBJECTAGG — basic
-- ============================================================================

-- begin-expected
-- columns: has_keys
-- row: true
-- end-expected
SELECT JSON_OBJECTAGG(
  JSON_VALUE(doc::text, '$.name') :
  JSON_VALUE(doc::text, '$.age' RETURNING integer)
) IS JSON OBJECT AS has_keys
FROM sj_data
WHERE doc IS NOT NULL;

-- ============================================================================
-- 38. JSON_OBJECTAGG — NULL handling
-- ============================================================================

-- begin-expected
-- columns: result
-- row: { "a" : 1 }
-- end-expected
SELECT JSON_OBJECTAGG(k : v ABSENT ON NULL) AS result
FROM (VALUES ('a', 1), ('b', NULL::integer)) AS t(k, v);

-- ============================================================================
-- 39. JSON_OBJECTAGG — WITH UNIQUE KEYS
-- ============================================================================

-- begin-expected-error
-- message-like: duplicate JSON object key value
-- end-expected-error
SELECT JSON_OBJECTAGG(k : v WITH UNIQUE KEYS) AS result
FROM (VALUES ('a', 1), ('a', 2)) AS t(k, v);

-- ============================================================================
-- JSON_ARRAY constructor
-- ============================================================================

-- ============================================================================
-- 40. JSON_ARRAY — static values
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [1, 2, 3]
-- end-expected
SELECT JSON_ARRAY(1, 2, 3) AS result;

-- ============================================================================
-- 41. JSON_ARRAY — mixed types
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [1, "hello", true, null]
-- end-expected
SELECT JSON_ARRAY(1, 'hello', true, NULL::text NULL ON NULL) AS result;

-- ============================================================================
-- 42. JSON_ARRAY ABSENT ON NULL
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [1, 3]
-- end-expected
SELECT JSON_ARRAY(1, NULL::integer, 3 ABSENT ON NULL) AS result;

-- ============================================================================
-- 43. JSON_ARRAY — from subquery
-- ============================================================================

-- begin-expected
-- columns: is_array
-- row: true
-- end-expected
SELECT JSON_ARRAY(SELECT val FROM (VALUES (1), (2), (3)) AS t(val)) IS JSON ARRAY AS is_array;

-- ============================================================================
-- JSON_OBJECT constructor
-- ============================================================================

-- ============================================================================
-- 44. JSON_OBJECT — key-value pairs
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {"a" : 1, "b" : "hello"}
-- end-expected
SELECT JSON_OBJECT('a' : 1, 'b' : 'hello') AS result;

-- ============================================================================
-- 45. JSON_OBJECT — NULL handling
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {"a" : 1}
-- end-expected
SELECT JSON_OBJECT('a' : 1, 'b' : NULL ABSENT ON NULL) AS result;

-- ============================================================================
-- 46. JSON_OBJECT — WITH UNIQUE KEYS
-- ============================================================================

-- begin-expected-error
-- message-like: duplicate JSON object key value
-- end-expected-error
SELECT JSON_OBJECT('a' : 1, 'a' : 2 WITH UNIQUE KEYS);

-- ============================================================================
-- Combined usage
-- ============================================================================

-- ============================================================================
-- 47. JSON functions in WHERE
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM sj_data
WHERE JSON_EXISTS(doc::text, '$.tags')
AND JSON_VALUE(doc::text, '$.name') = 'Alice';

-- ============================================================================
-- 48. JSON functions in CTE
-- ============================================================================

-- begin-expected
-- columns: name, age
-- row: Alice, 30
-- row: Bob, 25
-- row: Charlie, 35
-- end-expected
WITH extracted AS (
  SELECT
    JSON_VALUE(doc::text, '$.name') AS name,
    JSON_VALUE(doc::text, '$.age' RETURNING integer) AS age
  FROM sj_data
  WHERE doc IS NOT NULL
)
SELECT * FROM extracted ORDER BY name;

-- ============================================================================
-- 49. IS JSON on table column
-- ============================================================================

-- begin-expected
-- columns: id, is_json
-- row: 1, true
-- row: 2, true
-- row: 3, true
-- row: 4, NULL
-- end-expected
SELECT id, doc::text IS JSON AS is_json FROM sj_data ORDER BY id;

-- ============================================================================
-- 50. JSON_VALUE with PASSING
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 20
-- end-expected
SELECT JSON_VALUE('{"items":[10,20,30]}', '$.items[*] ? (@ == $target)' PASSING 20 AS target) AS result;

-- ============================================================================
-- Error cases
-- ============================================================================

-- ============================================================================
-- 51. JSON_VALUE on non-scalar (error with ERROR ON ERROR)
-- ============================================================================

-- begin-expected-error
-- message-like: JSON
-- end-expected-error
SELECT JSON_VALUE('{"a":{"b":1}}', '$.a' ERROR ON ERROR);

-- ============================================================================
-- 52. JSON_TABLE with invalid path
-- ============================================================================

-- note: Invalid JSON path expression should produce an error
-- begin-expected-error
-- message-like: JSON
-- end-expected-error
SELECT * FROM JSON_TABLE('{"a":1}', '$.[[invalid' COLUMNS (x text PATH '$') ERROR ON ERROR) jt;

-- ============================================================================
-- 53. NULL input to JSON functions
-- ============================================================================

-- begin-expected
-- columns: r1, r2
-- row: true, true
-- end-expected
SELECT
  JSON_VALUE(NULL, '$.a') IS NULL AS r1,
  JSON_QUERY(NULL, '$.a') IS NULL AS r2;

-- ============================================================================
-- 54. JSON_VALUE RETURNING boolean
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_VALUE('{"flag": true}', '$.flag' RETURNING boolean) AS result;

-- ============================================================================
-- 55. JSON_VALUE RETURNING numeric with precision
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 3.14
-- end-expected
SELECT JSON_VALUE('{"pi": 3.14159}', '$.pi' RETURNING numeric(5,2)) AS result;

-- ============================================================================
-- 56. JSON_VALUE RETURNING date
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2026-01-15
-- end-expected
SELECT JSON_VALUE('{"d": "2026-01-15"}', '$.d' RETURNING date) AS result;

-- ============================================================================
-- 57. JSON_VALUE type coercion failure: string to integer
-- ============================================================================

-- begin-expected-error
-- message-like: invalid
-- end-expected-error
SELECT JSON_VALUE('{"a": "hello"}', '$.a' RETURNING integer ERROR ON ERROR);

-- ============================================================================
-- 58. JSON_VALUE: extracting object (non-scalar) with ERROR ON ERROR
-- ============================================================================

-- begin-expected-error
-- message-like: JSON
-- end-expected-error
SELECT JSON_VALUE('{"a": {"nested": true}}', '$.a' ERROR ON ERROR);

-- ============================================================================
-- 59. JSON_VALUE: extracting array (non-scalar) with ERROR ON ERROR
-- ============================================================================

-- begin-expected-error
-- message-like: JSON
-- end-expected-error
SELECT JSON_VALUE('{"a": [1,2,3]}', '$.a' ERROR ON ERROR);

-- ============================================================================
-- 60. JSON_QUERY with CONDITIONAL WRAPPER
-- ============================================================================

-- note: CONDITIONAL WRAPPER wraps scalars/strings in array, leaves objects/arrays unwrapped

-- begin-expected
-- columns: result
-- row: 42
-- end-expected
SELECT JSON_QUERY('{"a": 42}', '$.a' WITH CONDITIONAL WRAPPER) AS result;

-- begin-expected
-- columns: result
-- row: [1, 2, 3]
-- end-expected
SELECT JSON_QUERY('{"a": [1,2,3]}', '$.a' WITH CONDITIONAL WRAPPER) AS result;

-- ============================================================================
-- 61. JSON_TABLE with EXISTS PATH column
-- ============================================================================

-- begin-expected
-- columns: id, name, has_email
-- row: 1, Alice, true
-- row: 2, Bob, false
-- end-expected
SELECT id, name, has_email FROM JSON_TABLE(
  '[{"id":1,"name":"Alice","email":"a@b.com"},{"id":2,"name":"Bob"}]',
  '$[*]' COLUMNS (
    id integer PATH '$.id',
    name text PATH '$.name',
    has_email boolean EXISTS PATH '$.email'
  )
) AS jt;

-- ============================================================================
-- 62. JSON_TABLE with nested NESTED PATH (two levels)
-- ============================================================================

-- begin-expected
-- columns: dept, team, member
-- row: eng, backend, Alice
-- row: eng, backend, Bob
-- row: eng, frontend, Carol
-- end-expected
SELECT dept, team, member FROM JSON_TABLE(
  '[{"dept":"eng","teams":[{"team":"backend","members":["Alice","Bob"]},{"team":"frontend","members":["Carol"]}]}]',
  '$[*]' COLUMNS (
    dept text PATH '$.dept',
    NESTED PATH '$.teams[*]' COLUMNS (
      team text PATH '$.team',
      NESTED PATH '$.members[*]' COLUMNS (
        member text PATH '$'
      )
    )
  )
) AS jt;

-- ============================================================================
-- 63. JSON_TABLE with sibling NESTED PATHs
-- ============================================================================

-- note: Two NESTED PATH at same level produce cross-product or UNION behavior

-- begin-expected
-- columns: name, tag
-- row: Alice, admin
-- row: Alice, user
-- end-expected
SELECT name, tag FROM JSON_TABLE(
  '{"name":"Alice","tags":["admin","user"]}',
  '$' COLUMNS (
    name text PATH '$.name',
    NESTED PATH '$.tags[*]' COLUMNS (
      tag text PATH '$'
    )
  )
) AS jt;

-- ============================================================================
-- 64. JSON_TABLE on empty array
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM JSON_TABLE(
  '[]',
  '$[*]' COLUMNS (x text PATH '$')
) AS jt;

-- ============================================================================
-- 65. JSONPATH wildcard: $.*
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT JSON_VALUE(val, '$') FROM JSON_TABLE(
    '{"a":1,"b":2,"c":3}',
    '$.*' COLUMNS (val text PATH '$')
  ) jt
) sub;

-- ============================================================================
-- 66. JSONPATH recursive descent: $..**
-- ============================================================================

-- note: $..name finds "name" at any nesting depth
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT JSON_EXISTS('{"a":{"b":{"name":"deep"}}}', '$..name') AS found;

-- ============================================================================
-- 67. JSONPATH filter expression: $.items[*] ? (@.price > 10)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_EXISTS(
  '{"items":[{"price":5},{"price":15},{"price":25}]}',
  '$.items[*] ? (@.price > 10)'
) AS result;

-- ============================================================================
-- 68. JSONPATH filter on string property
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_EXISTS(
  '{"users":[{"name":"Alice","role":"admin"},{"name":"Bob","role":"user"}]}',
  '$.users[*] ? (@.role == "admin")'
) AS result;

-- ============================================================================
-- 69. Deep nested path: $.a[0].b.c[1].d
-- ============================================================================

-- begin-expected
-- columns: result
-- row: deep
-- end-expected
SELECT JSON_VALUE(
  '{"a":[{"b":{"c":[null,{"d":"deep"}]}}]}',
  '$.a[0].b.c[1].d'
) AS result;

-- ============================================================================
-- 70. Multiple PASSING variables
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_EXISTS(
  '{"items":[{"price":5},{"price":15},{"price":25}]}',
  '$.items[*] ? (@.price > $lo && @.price < $hi)' PASSING 10 AS lo, 20 AS hi
) AS result;

-- ============================================================================
-- 71. GROUP BY with JSON_ARRAYAGG
-- ============================================================================

CREATE TABLE sj_products (category text, name text);
INSERT INTO sj_products VALUES
  ('fruit', 'apple'), ('fruit', 'banana'),
  ('veg', 'carrot'), ('veg', 'daikon'), ('veg', 'eggplant');

-- begin-expected
-- columns: category, names
-- row: fruit, ["apple", "banana"]
-- row: veg, ["carrot", "daikon", "eggplant"]
-- end-expected
SELECT category, JSON_ARRAYAGG(name ORDER BY name) AS names
FROM sj_products
GROUP BY category
ORDER BY category;

-- ============================================================================
-- 72. GROUP BY with JSON_OBJECTAGG
-- ============================================================================

CREATE TABLE sj_settings (section text, key text, val text);
INSERT INTO sj_settings VALUES
  ('db', 'host', 'localhost'), ('db', 'port', '5432'),
  ('app', 'name', 'myapp'), ('app', 'debug', 'true');

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT section, JSON_OBJECTAGG(key : val ORDER BY key) AS settings
FROM sj_settings
GROUP BY section
ORDER BY section;

-- ============================================================================
-- 73. HAVING with JSON_ARRAYAGG
-- ============================================================================

-- begin-expected
-- columns: category, names
-- row: veg, ["carrot", "daikon", "eggplant"]
-- end-expected
SELECT category, JSON_ARRAYAGG(name ORDER BY name) AS names
FROM sj_products
GROUP BY category
HAVING count(*) > 2
ORDER BY category;

DROP TABLE sj_products;
DROP TABLE sj_settings;

-- ============================================================================
-- 74. NULL path expression
-- ============================================================================

-- begin-expected
-- columns: r1
-- row: true
-- end-expected
SELECT JSON_VALUE('{"a":1}', NULL) IS NULL AS r1;

-- ============================================================================
-- 75. NULL input to JSON_TABLE
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM JSON_TABLE(
  NULL,
  '$[*]' COLUMNS (x text PATH '$')
) AS jt;

-- ============================================================================
-- 76. NULL input to JSON_ARRAYAGG / JSON_OBJECTAGG
-- ============================================================================

CREATE TABLE sj_empty (v text);

-- begin-expected-error
-- message-like: could not convert
-- end-expected-error
SELECT COALESCE(JSON_ARRAYAGG(v), '[]'::jsonb) AS result FROM sj_empty;

DROP TABLE sj_empty;

-- ============================================================================
-- 77. JSON_EXISTS on empty object and empty array
-- ============================================================================

-- begin-expected
-- columns: r1, r2
-- row: false, false
-- end-expected
SELECT
  JSON_EXISTS('{}', '$.a') AS r1,
  JSON_EXISTS('[]', '$[0]') AS r2;

-- ============================================================================
-- 78. JSON_VALUE on empty object
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_VALUE('{}', '$.a') IS NULL AS result;

-- ============================================================================
-- 79. JSON_TABLE with ERROR ON ERROR on invalid JSON
-- ============================================================================

-- begin-expected-error
-- message-like: JSON
-- end-expected-error
SELECT * FROM JSON_TABLE(
  'not json at all',
  '$[*]' COLUMNS (x text PATH '$')
  ERROR ON ERROR
) AS jt;

-- ============================================================================
-- 80. JSON_ARRAY with empty subquery
-- ============================================================================

-- begin-expected
-- columns: result
-- row: NULL
-- end-expected
SELECT JSON_ARRAY(SELECT v FROM (SELECT 1 AS v WHERE FALSE) sub) AS result;

-- ============================================================================
-- 81. JSON_OBJECT with ABSENT ON NULL vs NULL ON NULL
-- ============================================================================

-- begin-expected
-- columns: absent_result
-- row: {}
-- end-expected
SELECT JSON_OBJECT('a' : NULL ABSENT ON NULL) AS absent_result;

-- begin-expected
-- columns: null_result
-- row: {"a" : null}
-- end-expected
SELECT JSON_OBJECT('a' : NULL NULL ON NULL) AS null_result;

-- ============================================================================
-- 82. IS JSON with deeply nested duplicate keys
-- ============================================================================

-- note: WITH UNIQUE KEYS checks top-level keys only in standard SQL
-- begin-expected
-- columns: r1, r2
-- row: true, false
-- end-expected
SELECT
  '{"a":{"x":1},"b":{"x":2}}' IS JSON OBJECT WITH UNIQUE KEYS AS r1,
  '{"a":1,"a":2}' IS JSON OBJECT WITH UNIQUE KEYS AS r2;

-- ============================================================================
-- 83. IS JSON on various non-JSON strings
-- ============================================================================

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: false, false, false, false
-- end-expected
SELECT
  '' IS JSON AS r1,
  'undefined' IS JSON AS r2,
  '{key: value}' IS JSON AS r3,
  'NaN' IS JSON AS r4;

-- ============================================================================
-- 84. JSON_VALUE DEFAULT ON EMPTY combined with ERROR ON ERROR
-- ============================================================================

-- note: Missing path triggers ON EMPTY, not ON ERROR
-- begin-expected
-- columns: result
-- row: fallback
-- end-expected
SELECT JSON_VALUE('{"a":1}', '$.missing' DEFAULT 'fallback' ON EMPTY ERROR ON ERROR) AS result;

-- ============================================================================
-- 85. JSON_QUERY OMIT QUOTES on non-string
-- ============================================================================

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT JSON_QUERY('{"a": 42}', '$.a' OMIT QUOTES WITH WRAPPER) AS result;

-- ============================================================================
-- 86. JSON_OBJECTAGG duplicate key error with WITH UNIQUE KEYS
-- ============================================================================

CREATE TABLE sj_dupes (k text, v integer);
INSERT INTO sj_dupes VALUES ('a', 1), ('b', 2), ('a', 3);

-- begin-expected-error
-- message-like: duplicate
-- end-expected-error
SELECT JSON_OBJECTAGG(k : v WITH UNIQUE KEYS) FROM sj_dupes;

DROP TABLE sj_dupes;

-- ============================================================================
-- 87. JSON_SERIALIZE with RETURNING text
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_SERIALIZE(NULL::jsonb) IS NULL AS result;

-- ============================================================================
-- 88. JSON_SCALAR with various types
-- ============================================================================

-- begin-expected
-- columns: r1, r2, r3, r4, r5
-- row: "hello", 42, 3.14, true, null
-- end-expected
SELECT
  JSON_SCALAR('hello')::text AS r1,
  JSON_SCALAR(42)::text AS r2,
  JSON_SCALAR(3.14)::text AS r3,
  JSON_SCALAR(true)::text AS r4,
  JSON_SCALAR(NULL)::text AS r5;

-- ============================================================================
-- 89. JSON_TABLE joined with regular table (LEFT JOIN)
-- ============================================================================

CREATE TABLE sj_users (id integer PRIMARY KEY, name text);
INSERT INTO sj_users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');

-- begin-expected
-- columns: name, score
-- row: Alice, 100
-- row: Bob, 200
-- row: Charlie, NULL
-- end-expected
SELECT u.name, jt.score FROM sj_users u
LEFT JOIN JSON_TABLE(
  '[{"id":1,"score":100},{"id":2,"score":200}]',
  '$[*]' COLUMNS (
    id integer PATH '$.id',
    score integer PATH '$.score'
  )
) AS jt ON u.id = jt.id
ORDER BY u.id;

DROP TABLE sj_users;

-- ============================================================================
-- 90. JSON_VALUE with RETURNING text from boolean
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_VALUE('{"a": true}', '$.a' RETURNING text) AS result;

-- ============================================================================
-- 91. IS JSON SCALAR on various inputs
-- ============================================================================

-- begin-expected
-- columns: r1, r2, r3, r4, r5
-- row: true, true, true, false, false
-- end-expected
SELECT
  '"hello"' IS JSON SCALAR AS r1,
  '42' IS JSON SCALAR AS r2,
  'true' IS JSON SCALAR AS r3,
  '{}' IS JSON SCALAR AS r4,
  '[]' IS JSON SCALAR AS r5;

-- ============================================================================
-- 92. JSON_VALUE on array element by index
-- ============================================================================

-- begin-expected
-- columns: first, last
-- row: a, c
-- end-expected
SELECT
  JSON_VALUE('["a","b","c"]', '$[0]') AS first,
  JSON_VALUE('["a","b","c"]', '$[2]') AS last;

-- ============================================================================
-- 93. JSON_VALUE out-of-bounds array index
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_VALUE('["a","b"]', '$[99]') IS NULL AS result;

-- ============================================================================
-- 94. JSON_ARRAY with RETURNING jsonb
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [1, "two", true]
-- end-expected
SELECT JSON_ARRAY(1, 'two', true RETURNING jsonb) AS result;

-- ============================================================================
-- 95. JSON functions on jsonb column (not just string literals)
-- ============================================================================

-- begin-expected
-- columns: name, has_tags
-- row: Alice, true
-- row: Bob, true
-- row: Charlie, false
-- end-expected
SELECT
  JSON_VALUE(doc, '$.name') AS name,
  JSON_EXISTS(doc, '$.tags') AS has_tags
FROM sj_data
WHERE doc IS NOT NULL
ORDER BY id;

-- begin-expected
-- columns: id, tags
-- row: 1, ["admin", "user"]
-- row: 2, ["user"]
-- end-expected
SELECT id, JSON_QUERY(doc, '$.tags') AS tags
FROM sj_data
WHERE JSON_EXISTS(doc, '$.tags')
ORDER BY id;

-- ============================================================================
-- 96. IS NOT JSON OBJECT / IS NOT JSON ARRAY / IS NOT JSON SCALAR
-- ============================================================================

-- begin-expected
-- columns: r1, r2, r3, r4, r5, r6
-- row: true, false, true, false, true, false
-- end-expected
SELECT
  '[1,2]' IS NOT JSON OBJECT AS r1,
  '{"a":1}' IS NOT JSON OBJECT AS r2,
  '{"a":1}' IS NOT JSON ARRAY AS r3,
  '[1,2]' IS NOT JSON ARRAY AS r4,
  '{"a":1}' IS NOT JSON SCALAR AS r5,
  '42' IS NOT JSON SCALAR AS r6;

-- ============================================================================
-- 97. JSON_VALUE RETURNING timestamp
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2026-06-15 14:30:00
-- end-expected
SELECT JSON_VALUE('{"ts": "2026-06-15 14:30:00"}', '$.ts' RETURNING timestamp) AS result;

-- ============================================================================
-- 98. JSON functions in UPDATE SET context
-- ============================================================================

CREATE TABLE sj_update_test (id integer PRIMARY KEY, name text, raw jsonb);
INSERT INTO sj_update_test VALUES (1, 'placeholder', '{"name": "Alice", "score": 99}');

UPDATE sj_update_test
SET name = JSON_VALUE(raw, '$.name')
WHERE id = 1;

-- begin-expected
-- columns: name
-- row: Alice
-- end-expected
SELECT name FROM sj_update_test WHERE id = 1;

DROP TABLE sj_update_test;

-- ============================================================================
-- 99. JSON_QUERY ERROR ON ERROR
-- ============================================================================

-- begin-expected-error
-- message-like: JSON
-- end-expected-error
SELECT JSON_QUERY('not valid json', '$.a' ERROR ON ERROR);

-- ============================================================================
-- 100. JSON_QUERY NULL ON EMPTY
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT JSON_QUERY('{"a":1}', '$.missing' NULL ON EMPTY) IS NULL AS result;

-- ============================================================================
-- 101. JSON_ARRAYAGG RETURNING jsonb
-- ============================================================================

CREATE TABLE sj_ret_agg (v integer);
INSERT INTO sj_ret_agg VALUES (1), (2), (3);

-- begin-expected
-- columns: result
-- row: [1, 2, 3]
-- end-expected
SELECT JSON_ARRAYAGG(v ORDER BY v RETURNING jsonb) AS result FROM sj_ret_agg;

DROP TABLE sj_ret_agg;

-- ============================================================================
-- 102. JSON_TABLE with FORMAT JSON on column
-- ============================================================================

-- begin-expected
-- columns: id, nested
-- row: 1, {"x": 10}
-- end-expected
SELECT id, nested FROM JSON_TABLE(
  '[{"id":1,"data":{"x":10}}]',
  '$[*]' COLUMNS (
    id integer PATH '$.id',
    nested jsonb FORMAT JSON PATH '$.data'
  )
) AS jt;

-- ============================================================================
-- 103. JSON_OBJECT with KEY ... VALUE syntax
-- ============================================================================

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT JSON_OBJECT(KEY 'x' VALUE 10, KEY 'y' VALUE 20) AS result;

-- Mixed with colon syntax should also work
-- begin-expected
-- columns: result
-- row: {"a" : 1}
-- end-expected
SELECT JSON_OBJECT('a' : 1) AS result;

-- ============================================================================
-- 104. JSON function in CHECK constraint
-- ============================================================================

CREATE TABLE sj_checked (
  id integer PRIMARY KEY,
  doc text,
  CONSTRAINT chk_is_json CHECK (doc IS JSON)
);

INSERT INTO sj_checked VALUES (1, '{"valid": true}');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM sj_checked;

-- Violating: not valid JSON
-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO sj_checked VALUES (2, 'not json');

-- More complex: require a field exists
CREATE TABLE sj_checked2 (
  id integer PRIMARY KEY,
  doc jsonb,
  CONSTRAINT chk_has_name CHECK (JSON_EXISTS(doc, '$.name'))
);

INSERT INTO sj_checked2 VALUES (1, '{"name": "Alice"}');

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO sj_checked2 VALUES (2, '{"age": 30}');

DROP TABLE sj_checked, sj_checked2;

-- ============================================================================
-- 105. JSON function in expression index
-- ============================================================================

CREATE TABLE sj_indexed (id integer PRIMARY KEY, doc jsonb);
INSERT INTO sj_indexed VALUES
  (1, '{"name": "Alice", "score": 90}'),
  (2, '{"name": "Bob", "score": 75}'),
  (3, '{"name": "Charlie", "score": 85}');

CREATE INDEX idx_sj_name ON sj_indexed ((JSON_VALUE(doc, '$.name' RETURNING text)));

-- Index should accelerate this query
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM sj_indexed WHERE JSON_VALUE(doc, '$.name' RETURNING text) = 'Bob';

DROP TABLE sj_indexed;

-- ============================================================================
-- 106. JSON function in STORED generated column
-- ============================================================================

CREATE TABLE sj_generated (
  id integer PRIMARY KEY,
  doc jsonb,
  extracted_name text GENERATED ALWAYS AS (JSON_VALUE(doc, '$.name' RETURNING text)) STORED
);

INSERT INTO sj_generated (id, doc) VALUES
  (1, '{"name": "Alice"}'),
  (2, '{"name": "Bob"}');

-- begin-expected
-- columns: id, extracted_name
-- row: 1, Alice
-- row: 2, Bob
-- end-expected
SELECT id, extracted_name FROM sj_generated ORDER BY id;

-- Update doc, generated column should update
UPDATE sj_generated SET doc = '{"name": "Alicia"}' WHERE id = 1;

-- begin-expected
-- columns: extracted_name
-- row: Alicia
-- end-expected
SELECT extracted_name FROM sj_generated WHERE id = 1;

DROP TABLE sj_generated;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA sj_test CASCADE;
SET search_path = public;
