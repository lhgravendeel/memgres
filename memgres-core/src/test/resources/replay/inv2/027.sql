-- source: investigation-2026-08.md
-- finding: 27
-- title: The jsonpath engine is a text-substitution walker, not a parser: it locates the filter separator with rest.indexOf('?'), handles exactly one filter shape (@[.pa
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "ab"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('["ab","cd"]', '$[*] ? (@ like_regex "a")');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[*] ? (!(@ > 2))');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[{"a":1},{"b":2}]', '$[*] ? (exists(@.a))');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "abc"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('["abc","xyz"]', '$[*] ? (@ starts with "a")');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: true
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[true,false]', '$[*] ? (@ == true)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: null
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[null,1]', '$[*] ? (@ == null)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 1
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[*] ? (@ == 2 - 1)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 1
-- row: 3
-- rowcount: 2
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[*] ? (@ % 2 == 1)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 1
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$ ? (@[0] == 1)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "AB"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('["AB"]', '$[*] ? (@ like_regex "ab" flag "i")');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 12
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('"12"', '$.integer()');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 1.5
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('"1.5"', '$.decimal()');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: true
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('true', '$.boolean()');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "123"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('123', '$.string()');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "2020-01-01"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('"2020-01-01"', '$.date()');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: true
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('{"a":1}', '$.a == 1');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: false
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('{"a":1}', '$.b == 1');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: true
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('{"a":1}', 'exists($.a)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- rowcount: 0
-- end-expected
SELECT jsonb_path_query('[12345678901234567890]', '$[*] ? (@ == 12345678901234567891)');
-- begin-expected
-- columns: text:text
-- row: $."a"[*]?(@ > 1)
-- rowcount: 1
-- end-expected
SELECT '$.a[*] ? (@ > 1)'::jsonpath::text;
-- begin-expected
-- columns: text:text
-- row: $?(@."a" == 1)
-- rowcount: 1
-- end-expected
SELECT '$ ? (@.a == 1)'::jsonpath::text;
-- begin-expected
-- columns: text:text
-- row: ($."a" + 1)
-- rowcount: 1
-- end-expected
SELECT '$.a + 1'::jsonpath::text;
-- begin-expected
-- columns: text:text
-- row: $."a"
-- rowcount: 1
-- end-expected
SELECT 'lax $.a'::jsonpath::text;
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "2020-01-02"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('"01-02-2020"', '$.datetime("MM-DD-YYYY")');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: "2020-01-01"
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('["2020-01-01","2019-01-01"]', '$[*].datetime() ? (@ > "2019-06-01".datetime())');
