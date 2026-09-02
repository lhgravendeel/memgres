-- source: review-2026-08.md
-- finding: Root cause 12: the SQL/JSON behaviour-clause parser is a stateless two-iteration loop
-- area: The JSON implementation, second pass
-- title: Root cause 12: the SQL/JSON behaviour-clause parser is a stateless two-iteration loop
-- begin-expected
-- columns: json_query:jsonb
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT JSON_QUERY('[1]', '$.a' EMPTY ARRAY ON ERROR);
-- begin-expected
-- columns: json_query:jsonb
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT JSON_QUERY('[1]', '$.a' EMPTY OBJECT ON ERROR);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EMPTY"
-- end-expected-error
SELECT JSON_VALUE('1', '$' NULL ON EMPTY NULL ON EMPTY);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "NULL"
-- end-expected-error
SELECT JSON_VALUE('1', '$' ERROR ON ERROR NULL ON EMPTY);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ERROR"
-- end-expected-error
SELECT JSON_VALUE('1', '$' ERROR ON ERROR ERROR ON ERROR);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DEFAULT"
-- end-expected-error
SELECT JSON_VALUE('1', '$' DEFAULT 9 ON ERROR DEFAULT 8 ON ERROR);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "NULL"
-- end-expected-error
SELECT JSON_QUERY('[1]', '$' NULL ON ERROR NULL ON EMPTY);
