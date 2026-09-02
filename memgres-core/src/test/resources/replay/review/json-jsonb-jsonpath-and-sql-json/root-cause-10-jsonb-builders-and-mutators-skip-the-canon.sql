-- source: review-2026-08.md
-- finding: Root cause 10: jsonb builders and mutators skip the canonicalisation step
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 10: jsonb builders and mutators skip the canonicalisation step
-- begin-expected
-- columns: jsonb_object:jsonb
-- row: {"a": "2", "b": "1"}
-- rowcount: 1
-- end-expected
SELECT jsonb_object('{b,1,a,2}');
-- begin-expected
-- columns: jsonb_object:jsonb
-- row: {"a": "2", "b": "1"}
-- rowcount: 1
-- end-expected
SELECT jsonb_object(ARRAY['b','a'], ARRAY['1','2']);
-- begin-expected
-- columns: jsonb_insert:jsonb
-- row: [{"a": 2, "b": 1}, 1, 2]
-- rowcount: 1
-- end-expected
SELECT jsonb_insert('[1,2]', '{0}', '{"b":1,"a":2}');
-- begin-expected
-- columns: jsonb_insert:jsonb
-- row: [100, 1, 2]
-- rowcount: 1
-- end-expected
SELECT jsonb_insert('[1,2]', '{0}', '1e2');
-- begin-expected
-- columns: jsonb_insert:jsonb
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_insert('{"a":1}', '{}', '2');
-- begin-expected
-- columns: json_object:jsonb
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT JSON_OBJECT('a': 1 RETURNING jsonb);
