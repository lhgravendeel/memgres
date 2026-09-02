-- source: investigation-2026-08.md
-- finding: 4
-- title: Comparison is by the shape of the two Java values rather than by the operator PostgreSQL would resolve. TypeCoercion.compare/areEqual promote any two Numbers th
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
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '' = ' ';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ts (t text);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_vf_ts VALUES ('a'),('a '),('a  '),('b');
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_ts WHERE t = 'a';
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_ts a JOIN zz_vf_ts b ON a.t = b.t;
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT t) FROM zz_vf_ts;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 9007199254740993::bigint = 9007199254740992::bigint;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_big (b bigint);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_big VALUES (9007199254740992),(9007199254740993);
-- begin-expected
-- columns: max:int8
-- row: 9007199254740993
-- rowcount: 1
-- end-expected
SELECT max(b) FROM zz_vf_big;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_big WHERE b > 9007199254740992;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid > '00000000-0000-0000-0000-000000000001'::uuid;
-- begin-expected
-- columns: u:uuid
-- row: 00000000-0000-0000-0000-000000000001
-- row: 7fffffff-ffff-ffff-ffff-ffffffffffff
-- row: 80000000-0000-0000-0000-000000000000
-- row: ffffffff-ffff-ffff-ffff-ffffffffffff
-- rowcount: 4
-- end-expected
SELECT u FROM (VALUES ('00000000-0000-0000-0000-000000000001'::uuid),('7fffffff-ffff-ffff-ffff-ffffffffffff'::uuid),('80000000-0000-0000-0000-000000000000'::uuid),('ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)) t(u) ORDER BY u;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_md (n numeric);
-- begin-expected
-- ok: 5
-- end-expected
INSERT INTO zz_vf_md VALUES (1.0),(1.00),(1.000),(2.0),(2.0);
-- begin-expected
-- columns: mode:numeric
-- row: 1.0
-- rowcount: 1
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY n) FROM zz_vf_md;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_e AS ENUM ('small','medium','big');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ec (a zz_vf_e, b zz_vf_e);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ec VALUES ('small','big');
-- begin-expected
-- columns: enum_cmp:int4
-- row: -1
-- rowcount: 1
-- end-expected
SELECT enum_cmp(a, b) FROM zz_vf_ec;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_f1 (a int, b int, PRIMARY KEY (a, b));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_f1 VALUES (1, 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_f2 (x int, y int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_f2 VALUES (7, NULL);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_f2 ADD CONSTRAINT zz_vf_f2_fk FOREIGN KEY (x, y) REFERENCES zz_vf_f1 (a, b);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_n1 (a numeric PRIMARY KEY);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_n1 VALUES (1.0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_n2 (x numeric);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_n2 VALUES (1.00);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_n2 ADD CONSTRAINT zz_vf_n2_fk FOREIGN KEY (x) REFERENCES zz_vf_n1 (a);
