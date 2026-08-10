DROP TABLE IF EXISTS zz_sb_a1 CASCADE;

DROP TABLE IF EXISTS zz_sb_a3 CASCADE;

DROP TABLE IF EXISTS zz_sb_a2 CASCADE;

DROP TABLE IF EXISTS zz_sb_js CASCADE;

DROP TABLE IF EXISTS zz_sb_mt CASCADE;

DROP TABLE IF EXISTS zz_sb_ms CASCADE;

DROP TABLE IF EXISTS zz_sb_sel CASCADE;

CREATE TABLE zz_sb_a1 (id int, a int[]);

CREATE TABLE zz_sb_a3 (id int, m int[]);

CREATE TABLE zz_sb_a2 (id int, a int[]);

CREATE TABLE zz_sb_js (b jsonb);

CREATE TABLE zz_sb_mt (id int PRIMARY KEY, v int);

CREATE TABLE zz_sb_ms (id int, v int);

CREATE TABLE zz_sb_sel (b bytea, a int[]);

DROP TABLE IF EXISTS zz_sb_ins CASCADE;

CREATE TABLE zz_sb_ins (i int, arr int[]);

INSERT INTO zz_sb_ins (i, arr[1]) VALUES (2, 5);

INSERT INTO zz_sb_a1 VALUES (1,'{1,2,3}'),(2,NULL);

INSERT INTO zz_sb_a3 VALUES (1,'{{1,2},{3,4}}');

INSERT INTO zz_sb_a2 VALUES (3,'{1,2,3}');

INSERT INTO zz_sb_js VALUES ('{}');

INSERT INTO zz_sb_mt VALUES (1,10),(2,20);

INSERT INTO zz_sb_ms VALUES (1,111),(2,222);

-- begin-expected
-- columns: array
-- row: 2
-- end-expected
SELECT (ARRAY[1,2,3])[2];

-- begin-expected
-- columns: array
-- row: 2
-- end-expected
SELECT (ARRAY[1,2,3])[1.5];

-- begin-expected
-- columns: array
-- row: 1
-- end-expected
SELECT (ARRAY[1,2,3])[1.4];

-- begin-expected
-- columns: array
-- row: NULL
-- end-expected
SELECT (ARRAY[1,2,3])[0];

-- begin-expected
-- columns: array
-- row: NULL
-- end-expected
SELECT (ARRAY[1,2,3])[1][1];

-- begin-expected
-- columns: array
-- row: NULL
-- end-expected
SELECT (ARRAY[[1,2],[3,4]])[1];

-- begin-expected
-- columns: array
-- row: 2
-- end-expected
SELECT (ARRAY[[1,2],[3,4]])[1][2];

-- begin-expected
-- columns: array
-- row: {2,3,4}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[2:4]::text;

-- begin-expected
-- columns: array
-- row: {{1},{3}}
-- end-expected
SELECT (ARRAY[[1,2],[3,4]])[1:2][1:1]::text;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[NULL:2] IS NULL;

-- begin-expected
-- columns: array
-- row: {1,2,3}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[:3]::text;

-- begin-expected
-- columns: array
-- row: {3,4,5}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[3:]::text;

-- begin-expected
-- columns: array_lower
-- row: 1
-- end-expected
SELECT array_lower((ARRAY[1,2,3,4,5])[2:4],1);

-- begin-expected
-- columns: pg_typeof|pg_typeof
-- row: integer|integer[]
-- end-expected
SELECT pg_typeof((ARRAY[1,2,3])[1])::text, pg_typeof((ARRAY[1,2,3])[1:2])::text;

-- begin-expected
-- columns: x
-- row: 1
-- end-expected
SELECT (ARRAY[1,2,3])[1] AS x;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: integer out of range
-- end-expected-error
SELECT (ARRAY[1,2,3])[4294967297];

-- begin-expected-error
-- sqlstate: 42804
-- message-like: ERROR: array subscript must have type integer
-- end-expected-error
SELECT (ARRAY[1,2,3])['x'::text];

-- begin-expected-error
-- sqlstate: 42804
-- message-like: ERROR: cannot subscript type text because it does not support subscripting
-- end-expected-error
SELECT ('abcdef'::text)[2];

-- begin-expected-error
-- sqlstate: 42804
-- message-like: ERROR: cannot subscript type json because it does not support subscripting
-- end-expected-error
SELECT ('{"a": 1}'::json)['a'];

-- begin-expected
-- columns: jsonb
-- row: 1
-- end-expected
SELECT ('{"a":1}'::jsonb)['a'];

-- begin-expected
-- columns: jsonb
-- row: 1
-- end-expected
SELECT ('[1,2,3]'::jsonb)[0];

-- begin-expected
-- columns: jsonb
-- row: 3
-- end-expected
SELECT ('[1,2,3]'::jsonb)[-1];

-- begin-expected
-- columns: point
-- row: 2
-- end-expected
SELECT ('(1,2)'::point)[1];

-- begin-expected
-- columns: point
-- row: NULL
-- end-expected
SELECT ('(1,2)'::point)[2];

-- begin-expected
-- columns: typname
-- row: _
-- end-expected
SELECT typname[0] FROM pg_type WHERE typname = '_int4';

UPDATE zz_sb_a1 SET a[0] = 99 WHERE id = 1;

UPDATE zz_sb_a1 SET a[2] = 99 WHERE id = 2;

-- begin-expected
-- columns: id|a
-- row: 1|[0:3]={99,1,2,3}
-- row: 2|[2:2]={99}
-- end-expected
SELECT id, a::text FROM zz_sb_a1 ORDER BY id;

UPDATE zz_sb_a3 SET m[1][1] = 99 WHERE id = 1;

-- begin-expected
-- columns: m
-- row: {{99,2},{3,4}}
-- end-expected
SELECT m::text FROM zz_sb_a3;

-- begin-expected-error
-- sqlstate: 2202E
-- message-like: ERROR: array subscript out of range
-- end-expected-error
UPDATE zz_sb_a3 SET m[3][1] = 99 WHERE id = 1;

-- begin-expected-error
-- sqlstate: 2202E
-- message-like: ERROR: source array too small
-- end-expected-error
UPDATE zz_sb_a2 SET a[2:3] = '{7}' WHERE id = 3;

-- begin-expected
-- columns: a
-- row: {1,2,3}
-- end-expected
SELECT a::text FROM zz_sb_a2;

UPDATE zz_sb_a2 SET a[2:3] = '{7,8}' WHERE id = 3;

-- begin-expected
-- columns: a
-- row: {1,7,8}
-- end-expected
SELECT a::text FROM zz_sb_a2;

UPDATE zz_sb_a2 SET a[id] = 50 WHERE id = 3;

-- begin-expected
-- columns: a
-- row: {1,7,50}
-- end-expected
SELECT a::text FROM zz_sb_a2;

UPDATE zz_sb_js SET b['c'] = '5';

-- begin-expected
-- columns: b|jsonb_typeof
-- row: {"c": 5}|number
-- end-expected
SELECT b::text, jsonb_typeof(b -> 'c') FROM zz_sb_js;

MERGE INTO zz_sb_mt t USING zz_sb_ms s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = t.id + 100, v = s.v;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: ERROR: duplicate key value violates unique constraint "zz_sb_mt_pkey"
-- end-expected-error
INSERT INTO zz_sb_mt VALUES (101, 999);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zz_sb_mt;

-- begin-expected
-- columns: id|v
-- row: 101|111
-- end-expected
SELECT id, v FROM zz_sb_mt WHERE id = 101 ORDER BY v;

INSERT INTO zz_sb_sel SELECT '\x00010203de'::bytea, ARRAY[1,2,3];

-- begin-expected
-- columns: b|a
-- row: \x00010203de|{1,2,3}
-- end-expected
SELECT b::text, a::text FROM zz_sb_sel;

DROP TABLE zz_sb_a1;

DROP TABLE zz_sb_a3;

DROP TABLE zz_sb_a2;

DROP TABLE zz_sb_js;

DROP TABLE zz_sb_mt;

DROP TABLE zz_sb_ms;

-- begin-expected
-- columns: i|arr
-- row: 2|{5}
-- end-expected
SELECT i, arr::text FROM zz_sb_ins;

-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT '[1,5]'::jsonb @> '[2,3]'::jsonb;

-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT '[1,10]'::jsonb @> '[5]'::jsonb;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '[1,2,3]'::jsonb @> '[1,2]'::jsonb;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: jsonb -> bigint
-- end-expected-error
SELECT '[1,2]'::jsonb -> 1::bigint;

-- begin-expected
-- columns: ?column?
-- row: (1,2)x
-- end-expected
SELECT ROW(1,2) || 'x';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type integer: "{1,2}"
-- end-expected-error
SELECT 1 IN ('{1,2}');

-- begin-expected
-- columns: point|pg_typeof
-- row: 2|double precision
-- end-expected
SELECT ('(1,2)'::point)[1], pg_typeof(('(1,2)'::point)[1])::text;

DROP TABLE zz_sb_ins;

DROP TABLE zz_sb_sel;

