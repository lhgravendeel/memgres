-- source: investigation.md
-- finding: 19
-- title: Pattern-matching edge cases
-- begin-expected-error
-- sqlstate: 22025
-- message-like: LIKE pattern must not end with escape character
-- end-expected-error
SELECT 'abc' LIKE 'ab\';
-- PG: 22025 pattern must not end with escape character | mg: false
-- begin-expected-error
-- sqlstate: 22025
-- message-like: LIKE pattern must not end with escape character
-- end-expected-error
SELECT 'abc' LIKE 'ab!' ESCAPE '!';
-- PG: 22025 | mg: false
-- begin-expected-error
-- sqlstate: 22025
-- message-like: LIKE pattern must not end with escape character
-- end-expected-error
SELECT 'abc' ILIKE 'ab\';
-- PG: 22025 | mg: false
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT E'a\nb' ~ '(?p)a.b';
-- PG: false | mg: 2201B invalid regular expression: Unknown inline modifier
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT E'a\nb' ~ '(?w)a.b';
-- PG: true  | mg: 2201B;
