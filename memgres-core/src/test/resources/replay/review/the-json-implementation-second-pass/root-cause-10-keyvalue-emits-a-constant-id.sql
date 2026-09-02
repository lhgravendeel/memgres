-- source: review-2026-08.md
-- finding: Root cause 10: .keyvalue() emits a constant id
-- area: The JSON implementation, second pass
-- title: Root cause 10: .keyvalue() emits a constant id
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT (kv ->> 'id')) FROM jsonb_path_query('[{"a":1},{"b":2}]','$[*].keyvalue()') kv;
