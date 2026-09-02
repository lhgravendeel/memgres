-- source: investigation-2026-08.md
-- finding: 48
-- title: The jsonpath filter is scanned by hand with no quote tracking and no operator precedence: the closing-paren scan and findTopLevelLogicalOp ignore string literal
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "a)b"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('["a)b","c"]',  '$[*] ? (@ == "a)b")');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "a||b"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('["a||b","c"]', '$[*] ? (@ == "a||b")');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "a&&b"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('["a&&b","c"]', '$[*] ? (@ == "a&&b")');
-- begin-expected
-- columns: jsonb_path_match:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_path_match('{"a":1,"b":2}', '$.a == 1 && $.b == 2');
-- begin-expected
-- columns: jsonb_path_match:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_path_match('{"a":1,"b":2}', '$.a == 1 || $.b == 9');
-- begin-expected
-- columns: jsonb_path_match:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_path_match('{"a":1,"b":2}', '($.a == 1 && $.b == 2)');
