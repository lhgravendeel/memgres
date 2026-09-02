-- source: investigation.md
-- finding: 5
-- title: JSON container-type errors are not raised
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot get array length of a non-array
-- end-expected-error
SELECT jsonb_array_length('{"a":1}'::jsonb);
-- PG: 22023 non-array   | mg: 0
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot get array length of a scalar
-- end-expected-error
SELECT jsonb_array_length('3'::jsonb);
-- PG: 22023 scalar      | mg: 0
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot call jsonb_object_keys on an array
-- end-expected-error
SELECT jsonb_object_keys('[1,2]'::jsonb);
-- PG: 22023 on an array | mg: NULL
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot extract elements from an object
-- end-expected-error
SELECT jsonb_array_elements('{"a":1}'::jsonb);
-- PG: 22023             | mg: NULL
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot delete from object using integer index
-- end-expected-error
SELECT '{"a":1}'::jsonb - 0;
-- PG: 22023 cannot delete by integer index | mg: unchanged
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot set path in scalar
-- end-expected-error
SELECT jsonb_set('1'::jsonb, '{a}', '2');
-- PG: 22023 cannot set path in scalar      | mg: unchanged
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: path element at position 1 is not an integer: "a"
-- end-expected-error
SELECT jsonb_set('[1,2]'::jsonb, '{a}', '9');
-- PG: 22P02 path element is not an integer | mg: unchanged;
