-- source: review-2026-08.md
-- finding: Root cause 5: STRICT is not honoured, and a JSON null is carried as the text `'null'`
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 5: STRICT is not honoured, and a JSON null is carried as the text `'null'`
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_set('{"a":1}', '{a}', NULL) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_insert('[1,2]', '{0}', NULL) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_set('{"a":1}', NULL, '1') IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (value IS NULL) FROM jsonb_each_text('{"a":null}'::jsonb);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (value IS NULL) FROM json_each_text('{"a":null}'::json);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_typeof(NULL::jsonb) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT json_typeof(NULL::json) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT JSON_SCALAR(NULL) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT JSON_SCALAR(NULL::text) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT JSON_VALUE(jsonb '{"a":"null"}', '$.a') IS NULL;
