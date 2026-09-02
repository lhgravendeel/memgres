-- source: review-2026-08.md
-- finding: Root cause 8: the `.**` recursive accessor is looked up as an object member called `**`
-- area: The JSON implementation, second pass
-- title: Root cause 8: the `.**` recursive accessor is looked up as an object member called `**`
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a": {"b": 1}}
-- row: {"b": 1}
-- row: 1
-- rowcount: 3
-- end-expected
SELECT jsonb_path_query('{"a":{"b":1}}', '$.**');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a": {"b": 1}}
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('{"a":{"b":1}}', '$.**{0}');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"b": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('{"a":{"b":1}}', '$.**{1}');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a": {"b": 1}}
-- row: {"b": 1}
-- rowcount: 2
-- end-expected
SELECT jsonb_path_query('{"a":{"b":1}}', '$.**{0 to 1}');
-- begin-expected
-- columns: jsonb_path_exists:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_path_exists('{"a":{"b":1}}', '$.**.b');
