-- source: review-2026-08.md
-- finding: Root cause 11: the IS JSON predicate is evaluated over `toString()` with an ad-hoc validator
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 11: the IS JSON predicate is evaluated over `toString()` with an ad-hoc validator
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot use type integer in IS JSON predicate
-- end-expected-error
SELECT 1 IS JSON;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot use type boolean in IS JSON predicate
-- end-expected-error
SELECT true IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '{"a":{"b":1,"b":2}}' IS JSON WITH UNIQUE KEYS;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[{"b":1,"b":2}]'     IS JSON WITH UNIQUE KEYS;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '1.' IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '.5' IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '+1' IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '1d' IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '1f' IS JSON;
