-- source: review-2026-08.md
-- finding: Root cause 2: the SQL/JSON behaviour clauses are parsed into the AST but not implemented
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 2: the SQL/JSON behaviour clauses are parsed into the AST but not implemented
-- begin-expected
-- columns: json_query:jsonb
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT JSON_QUERY(jsonb '{"a":[1,2]}', '$.a[*]' WITH CONDITIONAL WRAPPER);
-- begin-expected-error
-- sqlstate: 22034
-- message-like: JSON path expression in JSON_VALUE must return single scalar item
-- end-expected-error
SELECT JSON_VALUE(jsonb '[1,2]', '$[*]' ERROR ON ERROR);
-- begin-expected-error
-- sqlstate: 22034
-- message-like: JSON path expression in JSON_QUERY must return single item when no wrapper is requested
-- end-expected-error
SELECT JSON_QUERY(jsonb '{"a":[1,2]}', '$.a[*]' ERROR ON ERROR);
-- begin-expected-error
-- sqlstate: 22035
-- message-like: no SQL/JSON item found for specified path
-- end-expected-error
SELECT JSON_QUERY(jsonb '{"a":1}', '$.b' ERROR ON EMPTY);
-- begin-expected
-- columns: json_query:jsonb
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT JSON_QUERY(jsonb '{"a":"[1, 2]"}', '$.a' OMIT QUOTES);
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
-- columns: json_value:text
-- row: t
-- rowcount: 1
-- end-expected
SELECT JSON_VALUE(jsonb '{"a": true}', '$.a' RETURNING text);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT JSON_EXISTS(jsonb '{"a":1}', 'strict $.b' UNKNOWN ON ERROR) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '{"a":1}' IS JSON WITHOUT UNIQUE KEYS;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '{"a":1}' IS JSON OBJECT WITHOUT UNIQUE KEYS;
-- begin-expected-error
-- sqlstate: 22030
-- message-like: duplicate JSON object key value
-- end-expected-error
SELECT JSON('{"a":1,"a":2}' WITH UNIQUE KEYS);
-- begin-expected
-- columns: json_query:text
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT JSON_QUERY(jsonb '{"a":[1,2]}', '$.a' RETURNING text FORMAT JSON);
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
