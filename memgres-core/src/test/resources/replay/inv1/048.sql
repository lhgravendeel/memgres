-- source: investigation.md
-- finding: 48
-- title: jsonpath: arithmetic and error suppression (6 cases)
-- begin-expected
-- columns: jsonb_path_query_array:jsonb
-- row: [2, 3, 4]
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query_array('{"x":[2,3,4]}', '+ $.x');
-- PG: [2,3,4] | mg: 42601 syntax error
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 5
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[2]', '7 - $[0]');
-- PG: 5       | mg: 42601
-- begin-expected
-- columns: jsonb_path_query_array:jsonb
-- row: [-2, -3, -4]
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query_array('{"x":[2,3,4]}', '- $.x');
-- PG: [-2,-3,-4] | mg: 42601
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 8
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[4]', '2 * $[0]');
-- PG: 8       | mg: 42601
-- begin-expected-error
-- sqlstate: 22038
-- message-like: left operand of jsonpath operator + is not a single numeric value
-- end-expected-error
SELECT jsonb_path_query('{"x":[2,3,4]}', '$.x + 1');
--   PG: 22038 left operand is not a single numeric value | mg: 0 rows
-- begin-expected
-- columns: ?column?:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT '{"a":[1,2,3]}'::jsonb @? 'strict $.b';
-- PG: NULL | mg: 2203A does not contain key "b"
-- begin-expected
-- columns: jsonb_path_exists:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT jsonb_path_exists('{"a":[1,2,3]}', 'strict $.b', '{}', true);
-- PG: NULL | mg: 2203A;
