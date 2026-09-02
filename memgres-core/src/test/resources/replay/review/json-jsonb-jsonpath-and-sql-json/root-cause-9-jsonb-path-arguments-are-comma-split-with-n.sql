-- source: review-2026-08.md
-- finding: Root cause 9: jsonb path arguments are comma-split with no empty or quoting handling
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 9: jsonb path arguments are comma-split with no empty or quoting handling
-- begin-expected
-- columns: ?column?:jsonb
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT '{"a":1}'::jsonb #> '{}';
-- begin-expected
-- columns: ?column?:jsonb
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT '[1,2]'::jsonb   #> '{}';
-- begin-expected
-- columns: ?column?:text
-- row: {"a":1}
-- rowcount: 1
-- end-expected
SELECT '{"a":1}'::json  #>> '{}';
-- begin-expected
-- columns: ?column?:text
-- row: x
-- rowcount: 1
-- end-expected
SELECT '{"1":"x"}'::jsonb #>> '{1}';
-- begin-expected
-- columns: jsonb_extract_path_text:text
-- row: x
-- rowcount: 1
-- end-expected
SELECT jsonb_extract_path_text('{"1":"x"}'::jsonb, '1');
-- begin-expected
-- columns: ?column?:text
-- row: y
-- rowcount: 1
-- end-expected
SELECT '{"a":{"2":"y"}}'::jsonb #>> '{a,2}';
-- begin-expected
-- columns: ?column?:text
-- row: x
-- rowcount: 1
-- end-expected
SELECT '{"-1":"x"}'::jsonb #>> '{-1}';
-- begin-expected
-- columns: jsonb_set:jsonb
-- row: {"a": 1, "b c": 2}
-- rowcount: 1
-- end-expected
SELECT jsonb_set('{"a":1}', '{"b c"}', '2');
