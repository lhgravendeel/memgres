-- ============================================================================
-- Feature Comparison: jsonpath item methods, array accessors, and json_each
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A jsonpath may end in an item method -- .type(), .size(), .abs(), .keyvalue()
-- and friends -- and may subscript an array with a range, a comma list, or the
-- word last. lax mode, the default, unwraps an array for a method that wants
-- one value and clamps a subscript to what is there; strict mode does neither
-- and says the document is not shaped the way the path claims.
--
-- The json_each family returns records rather than a table, so it can be called
-- in the SELECT list, expanded with (...).* , or read field by field.
-- ============================================================================

-- ============================================================================
-- 1. .type() and .size() describe the item they are given
-- ============================================================================

-- begin-expected
-- columns: a
-- row: "number"
-- end-expected
SELECT jsonb_path_query('2', '$.type()') AS a;

-- begin-expected
-- columns: a
-- row: "string"
-- end-expected
SELECT jsonb_path_query('"abc"', '$.type()') AS a;

-- begin-expected
-- columns: a
-- row: "array"
-- end-expected
SELECT jsonb_path_query('[1,2]', '$.type()') AS a;

-- begin-expected
-- columns: a
-- row: "object"
-- end-expected
SELECT jsonb_path_query('{"a":1}', '$.type()') AS a;

-- begin-expected
-- columns: a
-- row: "null"
-- end-expected
SELECT jsonb_path_query('null', '$.type()') AS a;

-- begin-expected
-- columns: a
-- row: "boolean"
-- end-expected
SELECT jsonb_path_query('true', '$.type()') AS a;

-- Neither method looks inside an array, in either mode
-- begin-expected
-- columns: a
-- row: "array"
-- end-expected
SELECT jsonb_path_query('[1,2,3]', 'strict $.type()') AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$.size()') AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', 'strict $.size()') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT jsonb_path_query('[[1,2],[3]]', 'lax $.size()') AS a;

-- Anything that is not an array counts as one item
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('1', '$.size()') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('{"a":1}', '$.size()') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('"str"', '$.size()') AS a;

-- ============================================================================
-- 2. The arithmetic methods
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2.5
-- end-expected
SELECT jsonb_path_query('-2.5', '$.abs()') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT jsonb_path_query('-2', '$.abs()') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT jsonb_path_query('2.5', '$.floor()') AS a;

-- begin-expected
-- columns: a
-- row: -3
-- end-expected
SELECT jsonb_path_query('-2.5', '$.floor()') AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT jsonb_path_query('2.5', '$.ceiling()') AS a;

-- begin-expected
-- columns: a
-- row: -2
-- end-expected
SELECT jsonb_path_query('-2.5', '$.ceiling()') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT jsonb_path_query('2', '$.double()') AS a;

-- .double() also reads a numeric string
-- begin-expected
-- columns: a
-- row: 2.5
-- end-expected
SELECT jsonb_path_query('"2.5"', '$.double()') AS a;

-- lax unwraps an array for a method that wants one value
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', 'lax $.abs()') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', 'strict $[*].abs()') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT jsonb_path_query('{"a":[1,-2]}', '$.a[*].abs()') AS a;

-- strict does not, and names what it was handed instead
-- begin-expected-error
-- sqlstate: 22036
-- message-like: jsonpath item method .abs() can only be applied to a numeric value
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', 'strict $.abs()');

-- begin-expected-error
-- sqlstate: 22036
-- message-like: jsonpath item method .floor() can only be applied to a numeric value
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', 'strict $.floor()');

-- begin-expected-error
-- sqlstate: 22036
-- message-like: jsonpath item method .abs() can only be applied to a numeric value
-- end-expected-error
SELECT jsonb_path_query('"x"', '$.abs()');

-- begin-expected-error
-- sqlstate: 22036
-- message-like: jsonpath item method .abs() can only be applied to a numeric value
-- end-expected-error
SELECT jsonb_path_query('true', '$.abs()');

-- begin-expected-error
-- sqlstate: 22036
-- message-like: is invalid for type double precision
-- end-expected-error
SELECT jsonb_path_query('"abc"', '$.double()');

-- ============================================================================
-- 3. .keyvalue() turns every member into an object of its own
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {"id": 0, "key": "a", "value": 1}
-- end-expected
SELECT jsonb_path_query('{"a":1}', '$.keyvalue()') AS a;

-- begin-expected
-- columns: a
-- row: {"id": 0, "key": "a", "value": 1}
-- row: {"id": 0, "key": "b", "value": 2}
-- end-expected
SELECT jsonb_path_query('{"a":1,"b":2}', '$.keyvalue()') AS a;

-- begin-expected
-- columns: a
-- row: [{"id": 0, "key": "a", "value": 1}, {"id": 0, "key": "b", "value": 2}]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}','$.keyvalue()')::text AS a;

-- begin-expected
-- columns: a
-- row: "a"
-- end-expected
SELECT jsonb_path_query('{"a":1}','$.keyvalue().key') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('{"a":1}','$.keyvalue().value') AS a;

-- begin-expected-error
-- sqlstate: 2203C
-- message-like: jsonpath item method .keyvalue() can only be applied to an object
-- end-expected-error
SELECT jsonb_path_query('[1]', 'lax $.keyvalue()');

-- begin-expected-error
-- sqlstate: 2203C
-- message-like: jsonpath item method .keyvalue() can only be applied to an object
-- end-expected-error
SELECT jsonb_path_query('1', 'strict $.keyvalue()');

-- ============================================================================
-- 4. .datetime() keeps the shape the string came in with
-- ============================================================================

-- begin-expected
-- columns: a
-- row: "2020-01-01"
-- end-expected
SELECT jsonb_path_query('"2020-01-01"', '$.datetime()') AS a;

-- begin-expected
-- columns: a
-- row: "2020-01-01T10:00:00"
-- end-expected
SELECT jsonb_path_query('"2020-01-01T10:00:00"', '$.datetime()') AS a;

-- A space between date and time is the same instant written the other way
-- begin-expected
-- columns: a
-- row: "2020-01-01T10:00:00"
-- end-expected
SELECT jsonb_path_query('"2020-01-01 10:00:00"', '$.datetime()') AS a;

-- begin-expected
-- columns: a
-- row: "10:00:00"
-- end-expected
SELECT jsonb_path_query('"10:00:00"', '$.datetime()') AS a;

-- begin-expected
-- columns: a
-- row: "2020-01-01T10:00:00+00:00"
-- end-expected
SELECT jsonb_path_query('"2020-01-01T10:00:00Z"', '$.datetime()') AS a;

-- begin-expected
-- columns: a
-- row: ["2020-01-01"]
-- end-expected
SELECT jsonb_path_query_array('{"a":"2020-01-01"}', '$.a.datetime()')::text AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_exists('{"d":"2020-01-05"}','$.d.datetime() > "2020-01-01".datetime()') AS a;

-- ============================================================================
-- 5. Range, comma-list and last subscripts
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[1 to 2]') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[0 to 0]') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[0 to last]') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[0,2]') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[0 to 1, 2]') AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[last]') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[last-1]') AS a;

-- lax clamps a subscript to the elements that exist
-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[1 to 5]') AS a;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query('[1,2,3]', '$[last+1]') q;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query('[1,2,3]', '$[2 to 1]') q;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query('[]','$[0]') q;

-- strict refuses it instead
-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_query('[1]', 'strict $[1]');

-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]','strict $[3]');

-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', 'strict $[1 to 5]');

-- A range that runs backwards is out of bounds too
-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', 'strict $[2 to 1]');

-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_query('[]','strict $[last]');

-- lax indexes a non-array as an array of one, strict does not
-- begin-expected
-- columns: a
-- row: {"a": 1}
-- end-expected
SELECT jsonb_path_query('{"a":1}', 'lax $[0]') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('1', '$[last]') AS a;

-- begin-expected-error
-- sqlstate: 22039
-- message-like: jsonpath array accessor can only be applied to an array
-- end-expected-error
SELECT jsonb_path_query('{"a":1}', 'strict $[0]');

-- begin-expected-error
-- sqlstate: 22039
-- message-like: jsonpath array accessor can only be applied to an array
-- end-expected-error
SELECT jsonb_path_query('1', 'strict $[last]');

-- begin-expected-error
-- sqlstate: 22039
-- message-like: jsonpath wildcard array accessor can only be applied to an array
-- end-expected-error
SELECT jsonb_path_query('1', 'strict $[*]');

-- ============================================================================
-- 6. A member accessor needs an object under strict
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('[{"a":1}]','lax $.a') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT jsonb_path_query('[{"a":1},{"a":2}]','lax $.a') AS a;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query('{"a":1}', 'lax $.a.b') q;

-- begin-expected-error
-- sqlstate: 2203A
-- message-like: jsonpath member accessor can only be applied to an object
-- end-expected-error
SELECT jsonb_path_query('{"a":1}', 'strict $.a.b');

-- begin-expected-error
-- sqlstate: 2203A
-- message-like: jsonpath member accessor can only be applied to an object
-- end-expected-error
SELECT jsonb_path_query('[1]', 'strict $.a');

-- A key the object does not have is still reported as a missing key
-- begin-expected-error
-- sqlstate: 2203A
-- message-like: does not contain key "b"
-- end-expected-error
SELECT jsonb_path_query('{"a":1}', 'strict $.b');

-- ============================================================================
-- 7. Predicates, silence, and the two operators
-- ============================================================================

-- A step that cannot be walked makes the comparison unknown, not an error
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT jsonb_path_match('{"a":[1,2,3]}', 'strict $.b == 1') AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT jsonb_path_match('{"a":1}','strict $.b == 1') AS a;

-- With nothing to compare, lax answers false
-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT jsonb_path_match('{"a":1}','lax $.b == 1') AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_match('{"a":1}','$.a == 1') AS a;

-- One matching pair is enough
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_match('[1,2]','$[*] > 1') AS a;

-- A match needs exactly one boolean
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_match('{"a":true}', '$.a') AS a;

-- begin-expected-error
-- sqlstate: 22038
-- message-like: single boolean result is expected
-- end-expected-error
SELECT jsonb_path_match('{"a":1}', '$.a');

-- begin-expected-error
-- sqlstate: 22038
-- message-like: single boolean result is expected
-- end-expected-error
SELECT jsonb_path_match('{"a":1}','lax $.b');

-- begin-expected-error
-- sqlstate: 22038
-- message-like: single boolean result is expected
-- end-expected-error
SELECT jsonb_path_match('[true,true]','$[*]');

-- The silent flag and the operators answer NULL where they would have raised
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT '[1]'::jsonb @? 'strict $[1]' AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT jsonb_path_exists('[1]', 'strict $[1]', '{}', true) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT '{"a":1}'::jsonb @@ '$.a' AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT '{"a":1}'::jsonb @@ 'strict $.b == 1' AS a;

-- Without the flag the same path raises
-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_exists('[1]', 'strict $[1]');

-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_query_array('[1]','strict $[1]');

-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of bounds
-- end-expected-error
SELECT jsonb_path_query_first('[1]','strict $[1]');

-- A whole path made of a comparison always produces an item, so it exists
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_exists('{"a":1}','$.a == 2') AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_exists('{"a":1}','strict $.b == 1') AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT '{"a":1}'::jsonb @? '$.a == 2' AS a;

-- A filter selects, and can select nothing
-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT jsonb_path_exists('[1,2]','$[*] ? (@ > 5)') AS a;

-- ============================================================================
-- 8. json_each in the SELECT list
-- ============================================================================

-- begin-expected
-- columns: a
-- row: (a,1)
-- end-expected
SELECT jsonb_each('{"a":1}'::jsonb) AS a;

-- begin-expected
-- columns: a
-- row: (a,1)
-- end-expected
SELECT json_each('{"a":1}'::json) AS a;

-- begin-expected
-- columns: a
-- row: (a,1)
-- end-expected
SELECT jsonb_each_text('{"a":1}'::jsonb) AS a;

-- begin-expected
-- columns: a
-- row: (a,1)
-- end-expected
SELECT json_each_text('{"a":1}'::json) AS a;

-- begin-expected
-- columns: a
-- row: (a,1)
-- row: (b,2)
-- end-expected
SELECT jsonb_each('{"a":1,"b":2}'::jsonb) AS a;

-- A field is quoted when leaving it bare would move the commas
-- begin-expected
-- columns: a
-- row: (a,"""x y""")
-- end-expected
SELECT jsonb_each('{"a":"x y"}'::jsonb) AS a;

-- begin-expected
-- columns: a
-- row: (a,"x y")
-- end-expected
SELECT jsonb_each_text('{"a":"x y"}'::jsonb) AS a;

-- begin-expected
-- columns: a
-- row: (a,null)
-- row: (b,"[1, 2]")
-- row: (c,"{""d"": 1}")
-- end-expected
SELECT jsonb_each('{"a":null,"b":[1,2],"c":{"d":1}}'::jsonb) AS a;

-- A json null becomes SQL NULL in the _text form, and prints as an empty field
-- begin-expected
-- columns: a
-- row: (a,)
-- row: (b,"[1, 2]")
-- end-expected
SELECT jsonb_each_text('{"a":null,"b":[1,2]}'::jsonb) AS a;

-- An empty or NULL document produces no rows at all
-- begin-expected
-- columns: a
-- end-expected
SELECT jsonb_each('{}'::jsonb) AS a;

-- begin-expected
-- columns: a
-- end-expected
SELECT jsonb_each(NULL::jsonb) AS a;

-- begin-expected
-- columns: a
-- end-expected
SELECT json_each(NULL::json) AS a;

-- Anything that is not an object is a container error, worded per family
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot deconstruct an array as an object
-- end-expected-error
SELECT json_each('[1,2]'::json);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot deconstruct a scalar
-- end-expected-error
SELECT json_each('3'::json);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call jsonb_each on a non-object
-- end-expected-error
SELECT jsonb_each('3'::jsonb);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call jsonb_each on a non-object
-- end-expected-error
SELECT jsonb_each('"s"'::jsonb);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call jsonb_each on a non-object
-- end-expected-error
SELECT jsonb_each('[1,2]'::jsonb);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call jsonb_each_text on a non-object
-- end-expected-error
SELECT jsonb_each_text('[1,2]'::jsonb);

-- ============================================================================
-- 9. Expanding a record, and reading one field of it
-- ============================================================================

-- begin-expected
-- columns: key|value
-- row: a|1
-- end-expected
SELECT (jsonb_each('{"a":1}'::jsonb)).*;

-- begin-expected
-- columns: key
-- row: a
-- row: b
-- end-expected
SELECT (jsonb_each('{"a":1,"b":2}'::jsonb)).key;

-- begin-expected
-- columns: value
-- row: 1
-- row: 2
-- end-expected
SELECT (jsonb_each('{"a":1,"b":2}'::jsonb)).value;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: could not identify column "nosuch" in record data type
-- end-expected-error
SELECT (jsonb_each('{"a":1}'::jsonb)).nosuch;

-- Two sets in one row run to the longest, the shorter reading NULL past its end
-- begin-expected
-- columns: a|b
-- row: (a,1)|(b,2)
-- row: NULL|(c,3)
-- end-expected
SELECT jsonb_each('{"a":1}'::jsonb) AS a, jsonb_each('{"b":2,"c":3}'::jsonb) AS b;

-- ============================================================================
-- 10. json walks its members as written, jsonb as it stores them
-- ============================================================================

-- begin-expected
-- columns: a
-- row: (b,1)
-- row: (a,2)
-- end-expected
SELECT json_each('{"b":1,"a":2}'::json) AS a;

-- begin-expected
-- columns: a
-- row: b
-- row: aa
-- row: a
-- end-expected
SELECT json_object_keys('{"b":1,"aa":2,"a":3}'::json) AS a;

-- jsonb stores its keys shortest first
-- begin-expected
-- columns: a
-- row: (a,2)
-- row: (b,1)
-- end-expected
SELECT jsonb_each('{"b":1,"a":2}'::jsonb) AS a;

-- begin-expected
-- columns: a
-- row: a
-- row: b
-- row: aa
-- end-expected
SELECT jsonb_object_keys('{"b":1,"aa":2,"a":3}'::jsonb) AS a;

-- ============================================================================
-- 11. The shapes that must keep working
-- ============================================================================

-- begin-expected
-- columns: key|value
-- row: a|1
-- row: b|2
-- end-expected
SELECT * FROM jsonb_each('{"a":1,"b":2}'::jsonb);

-- begin-expected
-- columns: k|v
-- row: a|1
-- row: b|2
-- end-expected
SELECT k, v FROM jsonb_each('{"a":1,"b":2}'::jsonb) AS t(k,v);

-- begin-expected
-- columns: key|value
-- row: a|x
-- end-expected
SELECT * FROM json_each_text('{"a":"x"}'::json);

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('{"a":[1,2,3]}', '$.a[*]') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT jsonb_path_query('{"a":[1,2,3]}', '$.a[1]') AS a;

-- begin-expected
-- columns: a
-- row: [1, 2, 3]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3]', '$[*]')::text AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query_first('[1,2,3]', '$[*]') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1,2,3]','$[*] ? (@ > 1)') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT jsonb_path_query('{"a":{"b":2}}', 'strict $.a.b') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('{"a":1}','$.*') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('1', 'lax $[*]') AS a;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query('{"a":1}', 'lax $.b') q;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT '{"a":1}'::jsonb @? '$.a' AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT '{"a":1}'::jsonb @@ '$.a == 1' AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_exists('[1]', 'strict $[0]') AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT jsonb_path_exists('{"a":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 2)') AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT jsonb_array_elements('[1,2]'::jsonb) AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT generate_series(1,2) AS a;

-- A NULL document stays NULL through every path function
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT jsonb_path_query_first(NULL::jsonb,'$.a') AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT jsonb_path_query_array(NULL::jsonb,'$.a') AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT jsonb_path_exists(NULL::jsonb,'$.a') AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT jsonb_path_match(NULL::jsonb,'$.a == 1') AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT NULL::jsonb @? '$.a' AS a;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query(NULL::jsonb,'$.a') q;

-- ============================================================================
-- 12. Reading a real column, through a view, a subquery and a grouped query
-- ============================================================================

DROP TABLE IF EXISTS jpm_doc CASCADE;
CREATE TABLE jpm_doc (id int, doc jsonb);
INSERT INTO jpm_doc VALUES (1, '{"a":1,"b":2}'), (2, '[1,2,3]'), (3, NULL);
CREATE VIEW jpm_doc_v AS SELECT id, doc FROM jpm_doc;

-- begin-expected
-- columns: id|a
-- row: 1|["object"]
-- row: 2|["array"]
-- row: 3|NULL
-- end-expected
SELECT id, jsonb_path_query_array(doc,'$.type()')::text AS a FROM jpm_doc ORDER BY id;

-- begin-expected
-- columns: id|a
-- row: 1|[1]
-- row: 2|[3]
-- row: 3|NULL
-- end-expected
SELECT id, jsonb_path_query_array(doc,'$.size()')::text AS a FROM jpm_doc ORDER BY id;

-- begin-expected
-- columns: id|a
-- row: 1|[{"a": 1, "b": 2}]
-- row: 2|[1, 2]
-- row: 3|NULL
-- end-expected
SELECT id, jsonb_path_query_array(doc,'$[0 to 1]')::text AS a FROM jpm_doc ORDER BY id;

-- begin-expected
-- columns: id|a
-- row: 1|[{"a": 1, "b": 2}]
-- row: 2|[3]
-- row: 3|NULL
-- end-expected
SELECT id, jsonb_path_query_array(doc,'$[last]')::text AS a FROM jpm_doc ORDER BY id;

-- begin-expected
-- columns: id|a
-- row: 1|1
-- row: 2|NULL
-- row: 3|NULL
-- end-expected
SELECT id, jsonb_path_query_first(doc,'$.a')::text AS a FROM jpm_doc ORDER BY id;

-- begin-expected
-- columns: id|a
-- row: 1|true
-- row: 2|false
-- row: 3|NULL
-- end-expected
SELECT id, (jsonb_path_exists(doc,'$.a'))::text AS a FROM jpm_doc ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM jpm_doc WHERE doc @? '$.a' ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM jpm_doc WHERE jsonb_path_match(doc,'$.a == 1') ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM jpm_doc_v WHERE doc @? '$.a' ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM (SELECT id, doc FROM jpm_doc) sub WHERE sub.doc @? '$.a' ORDER BY id;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM jpm_doc WHERE doc @? '$.a';

-- begin-expected
-- columns: id|a
-- row: 1|["object"]
-- row: 2|["array"]
-- row: 3|NULL
-- end-expected
SELECT id, jsonb_path_query_array(doc,'$.type()')::text AS a FROM jpm_doc
GROUP BY id, doc ORDER BY id;

-- begin-expected
-- columns: a
-- row: [1]
-- row: [3]
-- row: NULL
-- end-expected
SELECT jsonb_path_query_array(doc,'$.size()')::text AS a FROM jpm_doc
ORDER BY jsonb_path_query_array(doc,'$.size()')::text;

-- A record set expands the row it sits in
-- begin-expected
-- columns: id|a
-- row: 1|(a,1)
-- row: 1|(b,2)
-- end-expected
SELECT id, jsonb_each(doc) AS a FROM jpm_doc WHERE id=1;

-- begin-expected
-- columns: key|value
-- row: a|1
-- row: b|2
-- end-expected
SELECT (jsonb_each(doc)).* FROM jpm_doc WHERE id=1;

-- begin-expected
-- columns: key
-- row: a
-- row: b
-- end-expected
SELECT (jsonb_each_text(doc)).key FROM jpm_doc WHERE id=1 ORDER BY 1;

-- begin-expected
-- columns: a
-- end-expected
SELECT jsonb_each(doc) AS a FROM jpm_doc WHERE id=3;

-- A row whose document is not an object is still a container error
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call jsonb_each on a non-object
-- end-expected-error
SELECT jsonb_each(doc) FROM jpm_doc ORDER BY 1;

-- begin-expected
-- columns: k|v
-- row: a|1
-- row: b|2
-- end-expected
SELECT k, v::text FROM (SELECT doc FROM jpm_doc WHERE id=1) s,
    jsonb_each(s.doc) AS e(k,v) ORDER BY k;

-- begin-expected
-- columns: k|v
-- row: a|1
-- row: b|2
-- end-expected
SELECT k, v::text FROM (SELECT doc FROM jpm_doc WHERE id=1) s,
    LATERAL jsonb_each(s.doc) AS e(k,v) ORDER BY k;

DROP VIEW jpm_doc_v;
DROP TABLE jpm_doc;
