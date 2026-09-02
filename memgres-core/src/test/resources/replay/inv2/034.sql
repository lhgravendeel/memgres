-- source: investigation-2026-08.md
-- finding: 34
-- title: A jsonb text[] path argument is parsed by splitting the array literal on commas with no handling of the empty array and no de-quoting of elements, and a path st
-- begin-expected
-- columns: jsonb_set:text
-- row: {"a": 1, "b c": 2}
-- rowcount: 1
-- end-expected
SELECT jsonb_set('{"a":1}', '{"b c"}', '2')::text;
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
SELECT '[1,2]'::jsonb #> '{}';
-- begin-expected
-- columns: ?column?:text
-- row: {"a":1}
-- rowcount: 1
-- end-expected
SELECT '{"a":1}'::json #>> '{}';
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
