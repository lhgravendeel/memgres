-- source: investigation.md
-- finding: 4
-- title: `numeric` has no Infinity or NaN
-- begin-expected
-- columns: numeric:numeric
-- row: Infinity
-- rowcount: 1
-- end-expected
SELECT 'Infinity'::numeric;
-- PG: Infinity | mg: 22P02 invalid input syntax
-- begin-expected
-- columns: ?column?:numeric
-- row: Infinity
-- rowcount: 1
-- end-expected
SELECT 'Infinity'::numeric + 1;
-- PG: Infinity | mg: 22P02
-- begin-expected
-- columns: ?column?:numeric
-- row: NaN
-- rowcount: 1
-- end-expected
SELECT 'Infinity'::numeric - 'Infinity'::numeric;
-- PG: NaN | mg: 22P02
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '-Infinity'::numeric < 'NaN'::numeric;
-- PG: true (NaN sorts highest) | mg: 22P02
-- begin-expected
-- columns: numeric:numeric
-- row: NaN
-- rowcount: 1
-- end-expected
SELECT 'NaN'::numeric(10,2);
-- PG: NaN | mg: 22P02
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot convert NaN to integer
-- end-expected-error
SELECT 'NaN'::numeric::int;
-- PG: 0A000 cannot convert NaN to integer | mg: 0
-- begin-expected
-- columns: sum:numeric
-- row: NaN
-- rowcount: 1
-- end-expected
SELECT sum(v) FROM (VALUES ('NaN'::numeric),(1::numeric)) x(v);
-- PG: NaN | mg: 42883 function sum(text) does not exist;
