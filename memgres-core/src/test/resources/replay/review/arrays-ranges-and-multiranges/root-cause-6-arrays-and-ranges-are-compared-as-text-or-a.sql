-- source: review-2026-08.md
-- finding: Root cause 6: arrays and ranges are compared as text, or as longs
-- area: Arrays, ranges and multiranges
-- title: Root cause 6: arrays and ranges are compared as text, or as longs
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c1 (a int[]);
-- begin-expected
-- ok: 5
-- end-expected
INSERT INTO zz_vf_c1 VALUES ('{1,2}'),('{1}'),('{}'),('{1,2,3}'),('{2}');
-- begin-expected
-- columns: min:_int4 | max:_int4
-- row: {} | {2}
-- rowcount: 1
-- end-expected
SELECT min(a), max(a) FROM zz_vf_c1;
-- begin-expected
-- columns: a:_int4
-- row: {}
-- row: {1}
-- row: {1,2}
-- row: {1,2,3}
-- row: {2}
-- rowcount: 5
-- end-expected
SELECT a FROM zz_vf_c1 GROUP BY a ORDER BY a;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_c3" does not exist
-- end-expected-error
INSERT INTO zz_vf_c3 VALUES (1, ARRAY['a,b']), (2, ARRAY['a','a']), (3, ARRAY['b']);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_c3" does not exist
-- end-expected-error
SELECT id, a FROM zz_vf_c3 ORDER BY a, id;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT ARRAY[1,NULL]::int[] < ARRAY[1,2]::int[];
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT ARRAY[1.0]::numeric[] @> ARRAY[1.00]::numeric[];
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '[1.5,2.5)'::numrange = '[1.50,2.50)'::numrange;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_nr" does not exist
-- end-expected-error
INSERT INTO zz_vf_nr VALUES (1, numrange(1.5,3.0)), (2, numrange(1.2,3.0)), (3, numrange(1.9,3.0));
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_nr" does not exist
-- end-expected-error
SELECT id FROM zz_vf_nr ORDER BY r, id;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT numrange(1.5,3.0) < numrange(1.9,3.0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ex1 (r int4range, EXCLUDE USING gist (r WITH &&));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ex1 VALUES ('[1,10)');
-- begin-expected-error
-- sqlstate: 23P01
-- message-like: conflicting key value violates exclusion constraint "zz_vf_ex1_r_excl"
-- end-expected-error
INSERT INTO zz_vf_ex1 VALUES ('[9,20)');
-- begin-expected
-- columns: a:text | b:text
-- row: a | b, c
-- row: a, b | c
-- rowcount: 2
-- end-expected
WITH RECURSIVE zz_vf_r(a, b) AS (
    SELECT 'a'::text, 'b, c'::text
  UNION
    SELECT 'a, b'::text, 'c'::text FROM zz_vf_r WHERE a = 'a'
) SELECT a, b FROM zz_vf_r ORDER BY a, b;
