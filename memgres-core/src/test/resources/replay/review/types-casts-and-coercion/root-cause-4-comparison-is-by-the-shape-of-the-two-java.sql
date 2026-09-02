-- source: review-2026-08.md
-- finding: Root cause 4: comparison is by the shape of the two Java values
-- area: Types, casts and coercion
-- title: Root cause 4: comparison is by the shape of the two Java values
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'abc ' = 'abc';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'a ' > 'a';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ts (t text);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_ts VALUES ('a'),('a '),('a  '),('b');
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_ts WHERE t = 'a';
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_ts a JOIN zz_ts b ON a.t = b.t;
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT t) FROM zz_ts;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 9007199254740993::bigint = 9007199254740992::bigint;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_big (b bigint);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_big VALUES (9007199254740992),(9007199254740993);
-- begin-expected
-- columns: max:int8
-- row: 9007199254740993
-- rowcount: 1
-- end-expected
SELECT max(b) FROM zz_big;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_big WHERE b > 9007199254740992;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid > '00000000-0000-0000-0000-000000000001'::uuid;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT 1 IN (1, 'a');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT 'a' = ANY(ARRAY[1]);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer[] = bigint[]
-- end-expected-error
SELECT '{1,2}'::int[] = '{1,2}'::bigint[];
-- begin-expected-error
-- sqlstate: 42809
-- message-like: op ANY/ALL (array) requires array on right side
-- end-expected-error
SELECT 1 = ANY(1);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "1.5"
-- end-expected-error
SELECT '1.5' > 1;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT ' 1' = 1;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "1.5"
-- end-expected-error
SELECT '1.5' * 2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f1 (a int, b int, PRIMARY KEY (a, b));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f1 VALUES (1, 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f2 (x int, y int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f2 VALUES (7, NULL);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_f2 ADD CONSTRAINT zz_f2_fk FOREIGN KEY (x, y) REFERENCES zz_f1 (a, b);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_md (n numeric);
-- begin-expected
-- ok: 5
-- end-expected
INSERT INTO zz_md VALUES (1.0),(1.00),(1.000),(2.0),(2.0);
-- begin-expected
-- columns: mode:numeric
-- row: 1.0
-- rowcount: 1
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY n) FROM zz_md;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_e AS ENUM ('small','medium','big');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ec (a zz_e, b zz_e);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_ec VALUES ('small','big');
-- begin-expected
-- columns: enum_cmp:int4
-- row: -1
-- rowcount: 1
-- end-expected
SELECT enum_cmp(a, b) FROM zz_ec;
