-- source: review-2026-08.md
-- finding: Root cause 9: `array_agg` advertises `text[]`, and `containsAggregate()` enumerates node types by hand
-- area: Arrays, ranges and multiranges
-- title: Root cause 9: `array_agg` advertises `text[]`, and `containsAggregate()` enumerates node types by hand
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ag" does not exist
-- end-expected-error
SELECT unnest(array_agg(v)) AS u FROM zz_vf_ag ORDER BY u;
-- v int, rows 10,20,30
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ag" does not exist
-- end-expected-error
SELECT to_json(array_agg(v)) FROM zz_vf_ag;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ag" does not exist
-- end-expected-error
SELECT to_jsonb(array_agg(v)) FROM zz_vf_ag;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ag" does not exist
-- end-expected-error
SELECT max(v) BETWEEN 1 AND 100 FROM zz_vf_ag;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ag" does not exist
-- end-expected-error
SELECT ARRAY[max(v)] FROM zz_vf_ag;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ag" does not exist
-- end-expected-error
SELECT 30 = ANY (ARRAY[max(v)]) FROM zz_vf_ag;
