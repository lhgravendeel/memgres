-- source: review-2026-08.md
-- finding: Root cause 1: a value is admitted into a JSON document on a guess about its spelling, never a parse
-- area: The JSON implementation, second pass
-- title: Root cause 1: a value is admitted into a JSON document on a guess about its spelling, never a parse
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT jsonb_set('{"a":1}'::jsonb, '{a}', 'NaN')::text;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT jsonb_set('{"a":1}'::jsonb, '{a}', 'Infinity')::text;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '1d')::text;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '0x1p3')::text;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT jsonb_typeof(jsonb_set('{"a":1}'::jsonb, '{a}', 'NaN') -> 'a');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT jsonb_insert('[1]'::jsonb, '{0}', 'NaN')::text;
-- begin-expected
-- columns: to_jsonb:text
-- row: ["[1,2]"]
-- rowcount: 1
-- end-expected
SELECT to_jsonb(ARRAY['[1,2]']::text[])::text;
-- begin-expected
-- columns: to_json:text
-- row: ["[1,2]"]
-- rowcount: 1
-- end-expected
SELECT to_json(ARRAY['[1,2]']::text[])::text;
-- begin-expected
-- columns: to_jsonb:text
-- row: ["[1,2]", "x"]
-- rowcount: 1
-- end-expected
SELECT to_jsonb(ARRAY['[1,2]','x']::text[])::text;
-- begin-expected
-- columns: to_jsonb:text
-- row: [["[1,2]"]]
-- rowcount: 1
-- end-expected
SELECT to_jsonb(ARRAY[ARRAY['[1,2]']]::text[][])::text;
-- begin-expected
-- columns: jsonb_typeof:text
-- row: string
-- rowcount: 1
-- end-expected
SELECT jsonb_typeof(to_jsonb(ARRAY['[1,2]']::text[]) -> 0);
