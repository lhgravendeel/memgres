-- source: review-2026-08.md
-- finding: Root cause 3: json_strip_nulls reads a json document through the jsonb key map
-- area: The JSON implementation, second pass
-- title: Root cause 3: json_strip_nulls reads a json document through the jsonb key map
-- begin-expected
-- columns: json_strip_nulls:text
-- row: {"a":1,"a":2}
-- rowcount: 1
-- end-expected
SELECT json_strip_nulls('{"a":1,"a":2}'::json)::text;
-- begin-expected
-- columns: json_strip_nulls:text
-- row: {"b":1,"a":2}
-- rowcount: 1
-- end-expected
SELECT json_strip_nulls('{"b":1,"a":2}'::json)::text;
-- begin-expected
-- columns: json_strip_nulls:text
-- row: {"b":{"c":1},"a":2}
-- rowcount: 1
-- end-expected
SELECT json_strip_nulls('{"b":{"d":null,"c":1},"a":2}'::json)::text;
