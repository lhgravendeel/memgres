-- source: investigation-2026-08.md
-- finding: 44
-- title: json_strip_nulls reads a json document through the jsonb key map: stripNulls's object arm calls parseObjectKeys (a sorting TreeMap) rather than the parseObjectK
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
