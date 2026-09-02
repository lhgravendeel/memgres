-- source: investigation-2026-08.md
-- finding: 52
-- title: PostgreSQL array literals reach the JSON builders through String.split: toList splits on every comma and strips one quote pair by hand, with no awareness of quo
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
