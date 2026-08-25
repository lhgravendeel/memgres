-- ============================================================================
-- -- A jsonpath is a path, not a text substitution.
-- --
-- -- The engine was a walker over the path's characters: it found its filter with indexOf('?'),
-- -- handled exactly one filter shape, carried a table of eight method names and silently selected
-- -- nothing for anything outside that shape. A path is a grammar -- roots, wildcards, any-level
-- -- descent, subscripts written as expressions, filters that nest and combine, arithmetic, and
-- -- twenty item methods -- and each of its accessors has a lax reading and a strict one that differ
-- -- in whether an array is unwrapped, a scalar wrapped, or a miss refused. Its filters answer in
-- -- three values, since two items of different kinds are not unequal but incomparable, and the
-- -- errors from walking a document are what the silent argument and the @? and @@ operators turn
-- -- into unknown. .datetime() and its typed relatives make a value that is a date rather than the
-- -- string it prints as, so what it may be compared with and cast to is decided by which of the
-- -- five shapes it has.
-- ============================================================================

-- setup
CREATE TABLE jpath_doc (id int, j jsonb);
INSERT INTO jpath_doc VALUES (1, '{"a":[{"b":[1,2,3]},{"b":[4]}]}'), (2, '{"a":[]}');

-- ============================================================================
-- A path is parsed as a grammar
-- ============================================================================
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [2, 3]
-- end-expected
SELECT jsonb_path_query_array('{"a":[{"b":[1,2,3]},{"b":[4]}]}', '$.a[*] ? (@.b.size() > 1).b[*] ? (@ > 1)');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [{"x": 1, "y": 2}]
-- end-expected
SELECT jsonb_path_query_array('[{"x":1,"y":2},{"x":3,"y":1}]', '$[*] ? (@.x > 0 && @.y > 1)');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [{"x": 1}, {"x": 5}]
-- end-expected
SELECT jsonb_path_query_array('[{"x":1},{"x":5}]', '$[*] ? (@.x < 2 || @.x > 4)');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [{"x": 1}]
-- end-expected
SELECT jsonb_path_query_array('[{"x":1},{"x":5}]', '$[*] ? (!(@.x > 2))');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('{"a.b":1}', '$."a.b"');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('{"a":1}', '$ . a');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [2]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}', '$.* ? (@ > 1)');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('{"a":{"b":{"c":1}}}', '$.**.c');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [{"b": {"c": 1}}]
-- end-expected
SELECT jsonb_path_query_array('{"a":{"b":{"c":1}}}', '$.**{1}');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('{"a":{"b":{"c":1}}}', '$.**{1 to 2}.c');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [2, 3]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3,4]', '$[1 to 2]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [4]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3,4]', '$[last]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [3]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3,4]', '$[last - 1]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1, 3, 4]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3,4]', '$[0, 2 to 3]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [2]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3]', '$[*] ? (@ > 1) ? (@ < 3)');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('{"a":1}', '$ /* the whole document */ .a');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [2]
-- end-expected
SELECT jsonb_path_query_array('[1,2]', '$[*] ? (@ > $m)', '{"m":1}');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('{"a":[1,2]}', '$.a[*] ? (@ >= $lo && @ <= $hi)', '{"lo":1,"hi":1}');

-- ============================================================================
-- Arithmetic is an expression, not a substitution
-- ============================================================================
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [3]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3]', '$[*] ? ((@ + 1) * 2 > 6)');
-- begin-expected
-- columns: jsonb_path_query
-- row: 2
-- end-expected
SELECT jsonb_path_query('{"a":5}', '$.a % 3');
-- begin-expected
-- columns: jsonb_path_query
-- row: 2.5000000000000000
-- end-expected
SELECT jsonb_path_query('{"a":5}', '$.a / 2');
-- begin-expected
-- columns: jsonb_path_query
-- row: -5
-- end-expected
SELECT jsonb_path_query('{"a":5}', '-$.a');
-- begin-expected
-- columns: jsonb_path_query
-- row: -1
-- row: -2
-- end-expected
SELECT jsonb_path_query('{"a":[1,2]}', '-$.a[*]');
-- begin-expected-error
-- sqlstate: 22012
-- end-expected-error
SELECT jsonb_path_query('{"a":1}', '$.a / 0');
-- begin-expected-error
-- sqlstate: 22038
-- end-expected-error
SELECT jsonb_path_query('{"a":[1,2]}', '$.a[*] + 1');

-- ============================================================================
-- A predicate answers in three values
-- ============================================================================
-- begin-expected
-- columns: jsonb_path_query
-- row: null
-- end-expected
SELECT jsonb_path_query('[1]', '$[*] > "a"');
-- begin-expected
-- columns: jsonb_path_query
-- row: true
-- end-expected
SELECT jsonb_path_query('[1]', '($[*] > "a") is unknown');
-- begin-expected
-- columns: jsonb_path_query
-- row: true
-- end-expected
SELECT jsonb_path_query('[1]', '$[*] != null');
-- begin-expected
-- columns: jsonb_path_query
-- row: true
-- end-expected
SELECT jsonb_path_query('[null]', '$[*] == null');
-- begin-expected
-- columns: jsonb_path_query
-- row: false
-- end-expected
SELECT jsonb_path_query('[null]', '$[*] > null');
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT jsonb_path_query('[[1]]', '$[*] == [1]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: ["abc"]
-- end-expected
SELECT jsonb_path_query_array('["abc","abd"]', '$[*] ? (@ starts with "abc")');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: ["abc"]
-- end-expected
SELECT jsonb_path_query_array('["abc","xbc"]', '$[*] ? (@ like_regex "^a")');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: ["ABC"]
-- end-expected
SELECT jsonb_path_query_array('["ABC","xbc"]', '$[*] ? (@ like_regex "^a" flag "i")');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [{"a": 1}]
-- end-expected
SELECT jsonb_path_query_array('[{"a":1},{"b":1}]', '$[*] ? (exists (@.a))');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('[1,"a"]', '$[*] ? (@ == 1)');

-- ============================================================================
-- lax unwraps one level, strict does not
-- ============================================================================
-- begin-expected
-- columns: jsonb_path_query_array
-- row: ["array"]
-- end-expected
SELECT jsonb_path_query_array('[[1,2]]', 'lax $[*].type()');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: ["array"]
-- end-expected
SELECT jsonb_path_query_array('{"a":[1,2]}', 'lax $.a.type()');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('[[{"a":1}]]', 'lax $[*].a');
-- begin-expected-error
-- sqlstate: 2203A
-- end-expected-error
SELECT jsonb_path_query_array('[[{"a":1}]]', 'strict $[*].a');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('1', 'lax $[*]');
-- begin-expected-error
-- sqlstate: 22039
-- end-expected-error
SELECT jsonb_path_query_array('1', 'strict $[*]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('1', 'lax $[0]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('1', 'lax $.size()');
-- begin-expected-error
-- sqlstate: 22039
-- end-expected-error
SELECT jsonb_path_query_array('1', 'strict $.size()');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: []
-- end-expected
SELECT jsonb_path_query_array('{"a":1}', 'lax $.b');
-- begin-expected-error
-- sqlstate: 2203A
-- end-expected-error
SELECT jsonb_path_query_array('{"a":1}', 'strict $.b');
-- begin-expected-error
-- sqlstate: 22033
-- end-expected-error
SELECT jsonb_path_query_array('[1,2]', 'strict $[5]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: []
-- end-expected
SELECT jsonb_path_query_array('[1,2]', 'lax $[5]');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1, 2]
-- end-expected
SELECT jsonb_path_query_array('[{"a":1},{"a":2}]', 'lax $.a');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [{"b": 1}]
-- end-expected
SELECT jsonb_path_query_array('{"a":{"b":1}}', 'strict $.*');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [[[1, 2]], [1, 2], 1, 2]
-- end-expected
SELECT jsonb_path_query_array('[[1,2]]', 'lax $.**');

-- ============================================================================
-- The silent argument and the operators turn a walk that failed into unknown
-- ============================================================================
-- begin-expected
-- columns: jsonb_path_query_array
-- row: []
-- end-expected
SELECT jsonb_path_query_array('{"a":1}', 'strict $.b', '{}', true);
-- begin-expected
-- columns: jsonb_path_query_first
-- row: NULL
-- end-expected
SELECT jsonb_path_query_first('{"a":1}', 'strict $.b', '{}', true);
-- begin-expected
-- columns: jsonb_path_exists
-- row: NULL
-- end-expected
SELECT jsonb_path_exists('{"a":1}', 'strict $.b', '{}', true);
-- begin-expected
-- columns: jsonb_path_match
-- row: NULL
-- end-expected
SELECT jsonb_path_match('{"a":1}', 'strict $.b == 1', '{}', true);
-- begin-expected-error
-- sqlstate: 2203A
-- end-expected-error
SELECT jsonb_path_exists('{"a":1}', 'strict $.b');
-- begin-expected
-- columns: ?column?
-- row: NULL
-- end-expected
SELECT '{"a":1}'::jsonb @? 'strict $.b';
-- begin-expected
-- columns: ?column?
-- row: NULL
-- end-expected
SELECT '{"a":1}'::jsonb @@ 'strict $.b == 1';
-- begin-expected-error
-- sqlstate: 22038
-- end-expected-error
SELECT jsonb_path_match('{"a":1}', '$.a');
-- begin-expected
-- columns: jsonb_path_match
-- row: t
-- end-expected
SELECT jsonb_path_match('{"a":[1,2]}', '$.a[*] == 1');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: NULL
-- end-expected
SELECT jsonb_path_query_array('{"a":1}', 'strict $.b', NULL, true);
-- begin-expected
-- columns: jsonb_path_query_array
-- row: NULL
-- end-expected
SELECT jsonb_path_query_array('{"a":1}', '$.a', '{}', NULL);
-- begin-expected
-- columns: jsonb_path_exists
-- row: NULL
-- end-expected
SELECT jsonb_path_exists('{"a":1}', '$.a', NULL);

-- ============================================================================
-- A path that does not parse is a syntax error, silent or not
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT jsonb_path_query_array('1', 'a', '{}', true);
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT jsonb_path_query_array('1', '$ &');
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT jsonb_path_query_array('1', '(1');
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT jsonb_path_query_array('1', '$ ? (last > 0)');
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT jsonb_path_query_array('1', '$.nosuchmethod()');
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT jsonb_path_query_array('[1,2]', '$[*] ? (@ > $x)', '{"y":1}');
-- begin-expected-error
-- sqlstate: 22023
-- end-expected-error
SELECT jsonb_path_query_array('[1,2]', '$[*] ? (@ > $x)', '1');

-- ============================================================================
-- An item method is applied to the kind of item it names
-- ============================================================================
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('"1.7"', '$.integer()');
-- begin-expected
-- columns: jsonb_path_query
-- row: 2
-- end-expected
SELECT jsonb_path_query('1.7', '$.integer()');
-- begin-expected
-- columns: jsonb_path_query
-- row: 1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
-- end-expected
SELECT jsonb_path_query('1e300', '$.double()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('"inf"', '$.double()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('"inf"', '$.number()');
-- begin-expected
-- columns: jsonb_path_query
-- row: 123.46
-- end-expected
SELECT jsonb_path_query('123.456', '$.decimal(5, 2)');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('123.456', '$.decimal(2, 1)');
-- begin-expected
-- columns: jsonb_path_query
-- row: 1.5
-- end-expected
SELECT jsonb_path_query('-1.5', '$.abs()');
-- begin-expected
-- columns: jsonb_path_query
-- row: -2
-- end-expected
SELECT jsonb_path_query('-1.5', '$.floor()');
-- begin-expected
-- columns: jsonb_path_query
-- row: -1
-- end-expected
SELECT jsonb_path_query('-1.5', '$.ceiling()');
-- begin-expected
-- columns: jsonb_path_query
-- row: true
-- end-expected
SELECT jsonb_path_query('"yes"', '$.boolean()');
-- begin-expected
-- columns: jsonb_path_query
-- row: true
-- end-expected
SELECT jsonb_path_query('2', '$.boolean()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "true"
-- end-expected
SELECT jsonb_path_query('true', '$.string()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "1.20"
-- end-expected
SELECT jsonb_path_query('1.20', '$.string()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('true', '$.integer()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('null', '$.string()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('"x"', '$.abs()');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [{"id": 0, "key": "a", "value": 1}, {"id": 0, "key": "b", "value": 2}]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}', '$.keyvalue()');
-- begin-expected-error
-- sqlstate: 2203C
-- end-expected-error
SELECT jsonb_path_query_array('1', '$.keyvalue()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('9223372036854775808', '$.bigint()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('2147483648', '$.integer()');
-- begin-expected
-- columns: jsonb_path_query
-- row: 2147483648
-- end-expected
SELECT jsonb_path_query('2147483648', '$.bigint()');

-- ============================================================================
-- A date is a value of its own, not the string it prints as
-- ============================================================================
-- begin-expected
-- columns: jsonb_path_query
-- row: "2020-01-02"
-- end-expected
SELECT jsonb_path_query('"2020-01-02"', '$.datetime()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "date"
-- end-expected
SELECT jsonb_path_query('"2020-01-02"', '$.datetime().type()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "time without time zone"
-- end-expected
SELECT jsonb_path_query('"12:00:00"', '$.time().type()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "time with time zone"
-- end-expected
SELECT jsonb_path_query('"12:00:00+05"', '$.time_tz().type()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "timestamp without time zone"
-- end-expected
SELECT jsonb_path_query('"2020-01-02 03:04:05"', '$.timestamp().type()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "timestamp with time zone"
-- end-expected
SELECT jsonb_path_query('"2020-01-02 03:04:05+05"', '$.timestamp_tz().type()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "2020-01-02"
-- end-expected
SELECT jsonb_path_query('"2020-01-02"', '$.datetime().string()');
-- begin-expected-error
-- sqlstate: 22031
-- end-expected-error
SELECT jsonb_path_query('"2020-01-02"', '$.date().date()');
-- begin-expected-error
-- sqlstate: 22036
-- end-expected-error
SELECT jsonb_path_query('"2020-01-02"', '$.datetime().double()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "2020-01-02T00:00:00"
-- end-expected
SELECT jsonb_path_query('"2020-01-02"', '$.timestamp()');
-- begin-expected-error
-- sqlstate: 22031
-- end-expected-error
SELECT jsonb_path_query('"2020-01-02"', '$.time()');
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT jsonb_path_query('"2020-01-02"', '$.timestamp_tz()');
-- begin-expected
-- columns: jsonb_path_query_tz
-- row: "2020-01-02T00:00:00+00:00"
-- end-expected
SELECT jsonb_path_query_tz('"2020-01-02"', '$.timestamp_tz()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "22:04:05+00:00"
-- end-expected
SELECT jsonb_path_query('"2020-01-02 03:04:05+05"', '$.time_tz()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "2020-12-01"
-- end-expected
SELECT jsonb_path_query('"12/2020"', '$.datetime("MM/YYYY")');
-- begin-expected
-- columns: jsonb_path_query
-- row: "date"
-- end-expected
SELECT jsonb_path_query('"12/2020"', '$.datetime("MM/YYYY").type()');
-- begin-expected-error
-- sqlstate: 22031
-- end-expected-error
SELECT jsonb_path_query('"nope"', '$.datetime()');
-- begin-expected-error
-- sqlstate: 22031
-- end-expected-error
SELECT jsonb_path_query('1', '$.datetime()');
-- begin-expected
-- columns: jsonb_path_query
-- row: "03:04:05.68"
-- end-expected
SELECT jsonb_path_query('"03:04:05.6789"', '$.time(2)');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? ("2020-01-01".date() == "2020-01-01T00:00:00".timestamp())');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? ("2020-01-01".date() < "2020-01-02T00:00:00".timestamp())');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: []
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? ("2020-01-01".date() == "12:00:00".time())');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? (("2020-01-01".date() == "12:00:00".time()) is unknown)');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: []
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? ("12:00:00+00".time_tz() == "13:00:00+01".time_tz())');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? ("12:00:00+00".time_tz() > "13:00:00+01".time_tz())');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: [1]
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? ("2020-01-01T12:00:00+00".timestamp_tz() == "2020-01-01T13:00:00+01".timestamp_tz())');
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT jsonb_path_query_array('[1]', '$[*] ? ("2020-01-01".date() == "2020-01-01T00:00:00+00".timestamp_tz())');
-- begin-expected
-- columns: jsonb_path_query_array
-- row: []
-- end-expected
SELECT jsonb_path_query_array('[1]', '$[*] ? ("2020-01-01".date() == 1)');

-- ============================================================================
-- The same path over a column of documents
-- ============================================================================
-- begin-expected
-- columns: id | jsonb_path_query_array
-- row: 1 | [3, 4]
-- row: 2 | []
-- end-expected
SELECT id, jsonb_path_query_array(j, '$.a[*].b[*] ? (@ > 2)') FROM jpath_doc ORDER BY id;
-- begin-expected
-- columns: id | jsonb_path_exists
-- row: 1 | t
-- row: 2 | f
-- end-expected
SELECT id, jsonb_path_exists(j, '$.a[0].b[*] ? (@ > 2)') FROM jpath_doc ORDER BY id;
-- begin-expected
-- columns: id | jsonb_path_query_first
-- row: 1 | 1
-- row: 2 | NULL
-- end-expected
SELECT id, jsonb_path_query_first(j, '$.a[*].b[*]') FROM jpath_doc ORDER BY id;
-- begin-expected
-- columns: id | v
-- row: 1 | 1
-- row: 1 | 2
-- row: 1 | 3
-- row: 1 | 4
-- end-expected
SELECT d.id, q.v FROM jpath_doc d CROSS JOIN LATERAL jsonb_path_query(d.j, '$.a[*].b[*]') AS q(v) ORDER BY d.id, q.v::text;

-- ============================================================================
-- The id of a key-value pair names the object the pair came out of
-- ============================================================================
-- begin-expected
-- columns: a
-- row: {"id": 0, "key": "a", "value": 1}
-- row: {"id": 0, "key": "b", "value": 2}
-- end-expected
SELECT jsonb_path_query('{"a":1,"b":2}', '$.keyvalue()')::text AS a;
-- begin-expected
-- columns: a
-- row: [{"id": 0, "key": "a", "value": 1}, {"id": 0, "key": "b", "value": 2}]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}', '$.keyvalue()')::text AS a;
-- begin-expected
-- columns: a
-- row: ["a", "b"]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}', '$.keyvalue().key')::text AS a;
-- begin-expected
-- columns: a
-- row: [1, 2]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}', '$.keyvalue().value')::text AS a;
-- begin-expected
-- columns: a
-- row: [0, 0]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}', '$.keyvalue().id')::text AS a;
-- begin-expected
-- columns: a
-- row: []
-- end-expected
SELECT jsonb_path_query_array('{}', '$.keyvalue()')::text AS a;
-- begin-expected
-- columns: a
-- row: [{"id": 0, "key": "a", "value": 1}]
-- end-expected
SELECT jsonb_path_query_array('{"a":1,"b":2}', '$.keyvalue() ? (@.key == "a")')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT jsonb_path_exists('{"a":1}', '$.keyvalue() ? (@.value == 1)')::text AS a;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(DISTINCT kv->'id')::text AS a FROM jsonb_array_elements(jsonb_path_query_array('{"a":1,"b":2,"c":3}', '$.keyvalue()')) kv;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT count(DISTINCT kv->'id')::text AS a FROM jsonb_array_elements(jsonb_path_query_array('[{"a":1},{"b":2}]', '$[*].keyvalue()')) kv;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT count(DISTINCT kv->'id')::text AS a FROM jsonb_array_elements(jsonb_path_query_array('[{"a":1},{"a":1}]', '$[*].keyvalue()')) kv;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT count(DISTINCT kv->'id')::text AS a FROM jsonb_array_elements(jsonb_path_query_array('{"a":{"b":1},"c":{"d":2}}', '$.*.keyvalue()')) kv;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT count(DISTINCT kv->'id')::text AS a FROM jsonb_array_elements(jsonb_path_query_array('{"a":{"b":1,"e":3},"c":{"d":2}}', '$.*.keyvalue()')) kv;
-- begin-expected-error
-- sqlstate: 2203C
-- message-like: jsonpath item method .keyvalue() can only be applied to an object
-- end-expected-error
SELECT jsonb_path_query_array('[1,2]', 'lax $.keyvalue()')::text AS a;
-- begin-expected-error
-- sqlstate: 2203C
-- message-like: jsonpath item method .keyvalue() can only be applied to an object
-- end-expected-error
SELECT jsonb_path_query('[1,2]', 'strict $.keyvalue()')::text AS a;
-- begin-expected-error
-- sqlstate: 2203C
-- message-like: jsonpath item method .keyvalue() can only be applied to an object
-- end-expected-error
SELECT jsonb_path_query_array('[{"a":1},2]', 'lax $[*].keyvalue()')::text AS a;

-- expected-divergence: the id is documented as an implementation detail, and PostgreSQL's is the
-- offset in bytes at which the containing object begins inside the binary jsonb. What is defined
-- is which pairs share one and which do not, which the counts just above measure; the numbers
-- themselves are a property of a storage format rather than of the document.
SELECT jsonb_path_query_array('[{"a":1},{"b":2}]', '$[*].keyvalue().id')::text AS a;

-- teardown
DROP TABLE jpath_doc;
