-- source: investigation.md
-- finding: 18
-- title: Set-returning function gaps
-- begin-expected
-- columns: key:text | value:json
-- row: a | 1
-- rowcount: 1
-- end-expected
SELECT * FROM ROWS FROM (json_each('{"a":1}'::json));
-- mg: 42883 json_each(text) does not exist
-- begin-expected
-- columns: key:text
-- row: a
-- row: b
-- rowcount: 2
-- end-expected
SELECT (jsonb_each('{"a":1,"b":2}'::jsonb)).key;
-- mg: 42883 jsonb_each(text) does not exist
-- begin-expected
-- columns: value:text
-- row: 1
-- rowcount: 1
-- end-expected
SELECT (json_each_text('{"a":"1"}'::json)).value;
-- mg: 42883
-- begin-expected
-- columns: jsonb_each:text
-- row: (a,1)
-- rowcount: 1
-- end-expected
SELECT jsonb_each('{"a":1}'::jsonb)::text;
-- PG: (a,1) | mg: 42883
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- row: 1
-- rowcount: 2
-- end-expected
SELECT 1 FROM (VALUES (1)) v(x) GROUP BY generate_series(1,2);
--   PG: 2 rows | mg: 0A000 set-returning functions are not allowed in GROUP BY
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT array_agg(unnest(ARRAY[1,2]));
--   PG: 0A000 aggregate calls cannot contain SRF calls | mg: {[1, 2]}
-- begin-expected-error
-- sqlstate: 42809
-- message-like: OVER specified, but generate_series is not a window function nor an aggregate function
-- end-expected-error
SELECT generate_series(1,2) OVER ();
--   PG: 42809 OVER specified but not a window function | mg: NULL
-- begin-expected
-- columns: ?column?:int4
-- row: 11
-- row: 13
-- rowcount: 2
-- end-expected
SELECT generate_series(1,2) + generate_series(10,11);
--   PG: 11, 13 (lockstep) | mg: 42883 operator does not exist: integer + integer[];
