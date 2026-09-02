-- source: investigation-2026-08.md
-- finding: 39
-- title: json (as opposed to jsonb) is parsed into a Map, so it is silently canonicalised where PostgreSQL keeps the document exactly as written.
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
