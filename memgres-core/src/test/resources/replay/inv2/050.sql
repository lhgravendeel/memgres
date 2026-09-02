-- source: investigation-2026-08.md
-- finding: 50
-- title: jsonb_pretty re-indents the characters it is given rather than reading them as jsonb, so a literal argument is never canonicalised.
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
