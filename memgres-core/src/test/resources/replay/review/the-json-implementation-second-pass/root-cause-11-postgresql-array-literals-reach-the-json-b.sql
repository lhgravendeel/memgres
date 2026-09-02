-- source: review-2026-08.md
-- finding: Root cause 11: PostgreSQL array literals reach the JSON builders through String.split
-- area: The JSON implementation, second pass
-- title: Root cause 11: PostgreSQL array literals reach the JSON builders through String.split
-- begin-expected
-- columns: jsonb_object:text
-- row: {"a,b": "1"}
-- rowcount: 1
-- end-expected
SELECT jsonb_object('{"a,b",1}')::text;
-- begin-expected
-- columns: json_object:text
-- row: {"a,b" : "1"}
-- rowcount: 1
-- end-expected
SELECT json_object('{"a,b",1}')::text;
-- begin-expected
-- columns: jsonb_object:text
-- row: {"a\"b": "1"}
-- rowcount: 1
-- end-expected
SELECT jsonb_object('{"a\"b",1}')::text;
-- begin-expected
-- columns: jsonb_object:text
-- row: {"ab": "1"}
-- rowcount: 1
-- end-expected
SELECT jsonb_object('{"a\b",1}')::text;
