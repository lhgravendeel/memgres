-- source: investigation-2026-08.md
-- finding: 51
-- title: .keyvalue() emits the literal "id": 0 for every pair, so the field that exists to identify the containing object cannot distinguish objects.
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT (kv ->> 'id')) FROM jsonb_path_query('[{"a":1},{"b":2}]','$[*].keyvalue()') kv;
