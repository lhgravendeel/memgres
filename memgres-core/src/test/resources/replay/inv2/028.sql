-- source: investigation-2026-08.md
-- finding: 28
-- title: The SQL/JSON (SQL:2016) clauses are parsed into the AST but the evaluator ignores or stubs them: JSON_QUERY's CONDITIONAL WRAPPER branch is an empty block, OMIT
-- begin-expected
-- columns: json_query:jsonb
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT JSON_QUERY(jsonb '{"a":[1,2]}', '$.a[*]' WITH CONDITIONAL WRAPPER);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT JSON_EXISTS(jsonb '{"a":1}', 'strict $.b' UNKNOWN ON ERROR) IS NULL;
-- begin-expected
-- columns: json_query:jsonb
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT JSON_QUERY(jsonb '{"a":"[1, 2]"}', '$.a' OMIT QUOTES);
-- begin-expected
-- columns: json_value:text
-- row: t
-- rowcount: 1
-- end-expected
SELECT JSON_VALUE(jsonb '{"a": true}', '$.a' RETURNING text);
-- begin-expected
-- columns: json_value:text
-- row: x"y
-- rowcount: 1
-- end-expected
SELECT JSON_VALUE(jsonb '{"a":"x\"y"}', '$.a');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT JSON_VALUE(jsonb '{"a":"x\ny"}', '$.a') = E'x\ny';
-- begin-expected
-- columns: json_object:json
-- row: {"a" : 1}
-- rowcount: 1
-- end-expected
SELECT JSON_OBJECT('a': 1);
-- begin-expected
-- columns: json_array:json
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT JSON_ARRAY(1,2);
