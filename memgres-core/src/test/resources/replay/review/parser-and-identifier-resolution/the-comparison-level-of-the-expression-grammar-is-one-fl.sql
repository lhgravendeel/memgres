-- source: review-2026-08.md
-- finding: The comparison level of the expression grammar is one flat, non-recursive step
-- area: Parser and identifier resolution
-- title: The comparison level of the expression grammar is one flat, non-recursive step
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ch (a int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_ch VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "<"
-- end-expected-error
SELECT a FROM zz_ch WHERE 1 < a < 0 ORDER BY a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "<"
-- end-expected-error
SELECT 1 < 0 < 5;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 1 = 1 IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 1 < 2 IS TRUE;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 1 = 1 IS DISTINCT FROM false;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 1 BETWEEN 0 AND 2 IS TRUE;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' LIKE ANY(ARRAY['a%','z%']);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'abc' LIKE ALL(ARRAY['a%','z%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' ~ ANY(ARRAY['^a','^z']);
-- begin-expected
-- columns: ?column?:int4
-- row: -4
-- rowcount: 1
-- end-expected
SELECT ~ 2 + 1;
-- begin-expected
-- columns: ?column?:int4
-- row: -11
-- rowcount: 1
-- end-expected
SELECT ~ 5 * 2;
-- begin-expected
-- columns: ?column?:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT @ -3 + 1;
