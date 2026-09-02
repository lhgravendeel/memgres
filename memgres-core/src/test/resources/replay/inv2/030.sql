-- source: investigation-2026-08.md
-- finding: 30
-- title: JsonFunctions carries its own JSON serializer that escapes only backslash and double quote, appends Numbers with sb.append(val), and falls back to val.toString(
-- begin-expected
-- columns: to_json:text
-- row: "a\tb"
-- rowcount: 1
-- end-expected
SELECT to_json('a'||chr(9)||'b')::text;
-- begin-expected
-- columns: to_jsonb:text
-- row: "a\tb"
-- rowcount: 1
-- end-expected
SELECT to_jsonb('a'||chr(9)||'b')::text;
-- begin-expected
-- columns: json_build_array:text
-- row: ["a\tb"]
-- rowcount: 1
-- end-expected
SELECT json_build_array('a'||chr(9)||'b')::text;
-- begin-expected
-- columns: json_agg:text
-- row: ["a\tb"]
-- rowcount: 1
-- end-expected
SELECT json_agg(x)::text FROM (VALUES ('a'||chr(9)||'b')) t(x);
-- begin-expected
-- columns: to_json:text
-- row: "a\u0001b"
-- rowcount: 1
-- end-expected
SELECT to_json('a'||chr(1)||'b')::text;
-- begin-expected
-- columns: length:int4
-- row: 10
-- rowcount: 1
-- end-expected
SELECT length(to_json('a'||chr(1)||'b')::text);
-- begin-expected
-- columns: jsonb_build_object:text
-- row: {"a\"b": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_build_object('a"b', 1)::text;
-- begin-expected
-- columns: json_build_object:text
-- row: {"a\"b" : 1}
-- rowcount: 1
-- end-expected
SELECT json_build_object('a"b', 1)::text;
-- begin-expected
-- columns: jsonb_build_object:text
-- row: {"a\\b": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_build_object('a\b', 1)::text;
-- begin-expected
-- columns: to_json:text
-- row: "Infinity"
-- rowcount: 1
-- end-expected
SELECT to_json('inf'::float8)::text;
-- begin-expected
-- columns: to_jsonb:text
-- row: "NaN"
-- rowcount: 1
-- end-expected
SELECT to_jsonb('NaN'::float8)::text;
-- begin-expected
-- columns: to_json:text
-- row: "2020-01-01T10:00:00"
-- rowcount: 1
-- end-expected
SELECT to_json('2020-01-01 10:00:00'::timestamp)::text;
-- begin-expected
-- columns: to_jsonb:text
-- row: "2020-01-01T10:00:00"
-- rowcount: 1
-- end-expected
SELECT to_jsonb('2020-01-01 10:00:00'::timestamp)::text;
-- begin-expected
-- columns: row_to_json:text
-- row: {"f1":"2020-01-01T10:00:00"}
-- rowcount: 1
-- end-expected
SELECT row_to_json(row('2020-01-01 10:00:00'::timestamp))::text;
