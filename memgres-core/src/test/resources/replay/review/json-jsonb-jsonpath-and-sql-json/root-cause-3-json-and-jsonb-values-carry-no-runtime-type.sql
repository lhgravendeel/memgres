-- source: review-2026-08.md
-- finding: Root cause 3: json and jsonb values carry no runtime type tag
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 3: json and jsonb values carry no runtime type tag
-- begin-expected
-- columns: ?column?:text
-- row: [1,2][3]
-- rowcount: 1
-- end-expected
SELECT u || v FROM unnest(ARRAY['[1,2]']) AS u, unnest(ARRAY['[3]']) AS v;
-- begin-expected
-- columns: ?column?:text
-- row: {"a":1}{"b":2}
-- rowcount: 1
-- end-expected
SELECT u || v FROM unnest(ARRAY['{"a":1}']) AS u, unnest(ARRAY['{"b":2}']) AS v;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: json @> json
-- end-expected-error
SELECT '{"a":1}'::json @> '{"a":1}'::json;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: json ? unknown
-- end-expected-error
SELECT '{"a":1}'::json ? 'a';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: json #- unknown
-- end-expected-error
SELECT '{"a":1}'::json #- '{a}';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: json @? unknown
-- end-expected-error
SELECT '{"a":1}'::json @? '$.a';
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE IF EXISTS zz_j;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_j (j json);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_j VALUES ('{"a":1}'),('{"a":1}');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type json
-- end-expected-error
SELECT count(DISTINCT j) FROM zz_j;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type json
-- end-expected-error
SELECT j::text FROM zz_j GROUP BY j;
-- begin-expected
-- columns: int4:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT '1.5'::jsonb::int;
-- begin-expected
-- columns: int4:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT '2.7'::jsonb::int;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot cast jsonb string to type integer
-- end-expected-error
SELECT '"x"'::jsonb::int;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot cast jsonb object to type integer
-- end-expected-error
SELECT '{"a":1}'::jsonb::int;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot cast jsonb numeric to type boolean
-- end-expected-error
SELECT '1'::jsonb::boolean;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type integer to jsonb
-- end-expected-error
SELECT 123::jsonb;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type json to integer
-- end-expected-error
SELECT '5'::json::int;
-- begin-expected
-- columns: int4:int4
-- row: 5
-- rowcount: 1
-- end-expected
SELECT '5'::jsonb::int;
-- begin-expected
-- columns: jsonb:jsonb
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT '{"a":1}'::text::jsonb;
-- begin-expected
-- columns: text:text
-- row: $."a"
-- rowcount: 1
-- end-expected
SELECT '$.a'::jsonpath::text;
-- begin-expected
-- columns: text:text
-- row: 2020-01-01 10:00:00
-- rowcount: 1
-- end-expected
SELECT ('2020-01-01 10:00:00'::timestamp)::text;
