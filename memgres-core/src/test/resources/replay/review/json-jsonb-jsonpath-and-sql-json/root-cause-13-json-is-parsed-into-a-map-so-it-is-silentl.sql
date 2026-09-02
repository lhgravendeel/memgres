-- source: review-2026-08.md
-- finding: Root cause 13: json is parsed into a Map, so it is silently canonicalised
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 13: json is parsed into a Map, so it is silently canonicalised
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM json_each('{"a":1,"a":2}'::json);
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM json_object_keys('{"a":1,"a":2}'::json);
