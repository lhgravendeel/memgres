-- source: investigation-2026-08.md
-- finding: 36
-- title: jsonb-producing builders and mutators return the text they assembled without running it through normalizedIfStructured/normalizeJsonb, so the result is not cano
-- begin-expected
-- columns: jsonb_object:text
-- row: {"a": "2", "b": "1"}
-- rowcount: 1
-- end-expected
SELECT jsonb_object('{b,1,a,2}')::text;
-- begin-expected
-- columns: jsonb_object:text
-- row: {"a": "2", "b": "1"}
-- rowcount: 1
-- end-expected
SELECT jsonb_object(ARRAY['b','a'], ARRAY['1','2'])::text;
-- begin-expected
-- columns: jsonb_insert:text
-- row: [{"a": 2, "b": 1}, 1, 2]
-- rowcount: 1
-- end-expected
SELECT jsonb_insert('[1,2]', '{0}', '{"b":1,"a":2}')::text;
-- begin-expected
-- columns: jsonb_insert:text
-- row: [100, 1, 2]
-- rowcount: 1
-- end-expected
SELECT jsonb_insert('[1,2]', '{0}', '1e2')::text;
-- begin-expected
-- columns: jsonb_insert:text
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_insert('{"a":1}', '{}', '2')::text;
-- begin-expected
-- columns: json_object:text
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT JSON_OBJECT('a': 1 RETURNING jsonb)::text;
