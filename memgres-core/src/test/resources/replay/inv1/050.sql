-- source: investigation.md
-- finding: 50
-- title: Conditional and comparison predicates (4 cases)
-- begin-expected
-- columns: nullif:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT NULLIF(1, NULL);
-- PG: 1 | mg: 22P02 invalid input syntax for type integer: ""
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unequal number of entries in row expressions
-- end-expected-error
SELECT ROW(1,2) < ROW(1,2,3);
-- PG: 42601 unequal number of entries | mg: true
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of IS UNKNOWN must be type boolean, not type integer
-- end-expected-error
SELECT 1 IS UNKNOWN;
-- PG: 42804 argument must be boolean | mg: f
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 1 ISNULL;
-- PG: false | mg: 42601 syntax error at or near "isnull"
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 1 NOTNULL;
-- PG: true  | mg: 42601;
