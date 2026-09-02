-- source: review-2026-08.md
-- finding: Root cause 9: jsonb_pretty re-indents the characters it is given rather than reading them as jsonb
-- area: The JSON implementation, second pass
-- title: Root cause 9: jsonb_pretty re-indents the characters it is given rather than reading them as jsonb
-- begin-expected
-- columns: jsonb_pretty:text
-- row: {\n    "a": 2,\n    "b": 1\n}
-- rowcount: 1
-- end-expected
SELECT jsonb_pretty('{"b":1,"a":2}');
-- begin-expected
-- columns: jsonb_pretty:text
-- row: 100
-- rowcount: 1
-- end-expected
SELECT jsonb_pretty('1e2');
-- begin-expected
-- columns: jsonb_pretty:text
-- row: {\n    "a": 2\n}
-- rowcount: 1
-- end-expected
SELECT jsonb_pretty('{"a":1,"a":2}');
