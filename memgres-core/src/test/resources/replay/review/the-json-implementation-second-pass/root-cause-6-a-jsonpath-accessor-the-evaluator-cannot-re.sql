-- source: review-2026-08.md
-- finding: Root cause 6: a jsonpath accessor the evaluator cannot read is skipped, so the whole container is returned
-- area: The JSON implementation, second pass
-- title: Root cause 6: a jsonpath accessor the evaluator cannot read is skipped, so the whole container is returned
-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of integer range
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', '$[99999999999]');
-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of integer range
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', 'strict $[99999999999]');
-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is out of integer range
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', '$[0 to 99999999999]');
-- begin-expected-error
-- sqlstate: 22033
-- message-like: jsonpath array subscript is not a single numeric value
-- end-expected-error
SELECT jsonb_path_query('[1,2,3]', '$["a"]');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: 2
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[1,2,3]', '$[1.5]');
