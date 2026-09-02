-- source: investigation-2026-08.md
-- finding: 26
-- title: Unrelated singletons in this area
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
SELECT ARRAY[1.00]::numeric[] && ARRAY[1.000]::numeric[];
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '[1.5,2.5)'::numrange = '[1.50,2.50)'::numrange;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '[1.0,2.0)'::numrange = '[1,2)'::numrange;
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
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_ex1;
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
-- begin-expected
-- columns: array_to_json:json
-- row: [1,2]
-- rowcount: 1
-- end-expected
SELECT array_to_json(ARRAY[1,2]);
-- begin-expected
-- columns: array_to_json:json
-- row: [[1,2],[3,4]]
-- rowcount: 1
-- end-expected
SELECT array_to_json(ARRAY[[1,2],[3,4]]);
-- begin-expected
-- columns: array_to_json:json
-- row: [1,\n 2]
-- rowcount: 1
-- end-expected
SELECT array_to_json(ARRAY[1,2], true);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ab' LIKE ANY (ARRAY['a%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ab' ILIKE ANY (ARRAY['A%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ab' ~ ANY (ARRAY['^a']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ab' NOT LIKE ALL (ARRAY['z%']);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ad (a int[3], b text ARRAY, c text ARRAY[4]);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 1 < 2 IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT ARRAY[1,2] @> ARRAY[1] IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[1,10)'::int4range @> 5 IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 1 = ANY (ARRAY[1,2]) IS NULL;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ins (i int, arr int[]);
-- begin-expected
-- columns: i:int4 | arr:_int4
-- row: 2 | {5}
-- rowcount: 1
-- end-expected
INSERT INTO zz_vf_ins (i, arr[1]) VALUES (2, 5) RETURNING i, arr;
