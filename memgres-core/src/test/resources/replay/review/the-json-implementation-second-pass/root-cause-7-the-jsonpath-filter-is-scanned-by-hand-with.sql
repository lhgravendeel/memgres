-- source: review-2026-08.md
-- finding: Root cause 7: the jsonpath filter is scanned by hand, with no quote tracking and no operator precedence
-- area: The JSON implementation, second pass
-- title: Root cause 7: the jsonpath filter is scanned by hand, with no quote tracking and no operator precedence
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
