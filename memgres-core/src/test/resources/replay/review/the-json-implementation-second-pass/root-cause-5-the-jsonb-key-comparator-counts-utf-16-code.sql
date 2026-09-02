-- source: review-2026-08.md
-- finding: Root cause 5: the jsonb key comparator counts UTF-16 code units, PostgreSQL counts UTF-8 bytes
-- area: The JSON implementation, second pass
-- title: Root cause 5: the jsonb key comparator counts UTF-16 code units, PostgreSQL counts UTF-8 bytes
-- begin-expected
-- columns: text:text
-- row: {"ab": 1, "é": 2}
-- rowcount: 1
-- end-expected
SELECT '{"ab":1,"é":2}'::jsonb::text;
-- begin-expected
-- columns: jsonb_object_keys:text
-- row: ab
-- row: é
-- rowcount: 2
-- end-expected
SELECT jsonb_object_keys('{"ab":1,"é":2}'::jsonb);
-- begin-expected
-- columns: key:text
-- row: ab
-- row: é
-- rowcount: 2
-- end-expected
SELECT key FROM jsonb_each('{"ab":1,"é":2}'::jsonb);
-- begin-expected
-- columns: jsonb_build_object:text
-- row: {"ab": 1, "é": 2}
-- rowcount: 1
-- end-expected
SELECT jsonb_build_object('ab', 1, 'é', 2)::text;
-- begin-expected
-- columns: text:text
-- row: {"ab": 1, "é": 2}
-- rowcount: 1
-- end-expected
SELECT ('{"ab":1}'::jsonb || '{"é":2}'::jsonb)::text;
-- begin-expected
-- columns: text:text
-- row: {"abc": 1, "€": 2}
-- rowcount: 1
-- end-expected
SELECT '{"abc":1,"€":2}'::jsonb::text;
-- begin-expected
-- columns: text:text
-- row: {"abcd": 1, "😀": 2}
-- rowcount: 1
-- end-expected
SELECT '{"abcd":1,"😀":2}'::jsonb::text;
