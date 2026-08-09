-- begin-expected
-- columns: a|b|c
-- row: f|t|f
-- end-expected
SELECT 'abc ' = 'abc' AS a, 'a ' > 'a' AS b, '' = ' ' AS c;

-- begin-expected
-- columns: a|b
-- row: t|t
-- end-expected
SELECT 'ab'::char(5) = 'ab' AS a, 'ab'::char(5) = 'ab   ' AS b;

-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT 9007199254740993::bigint = 9007199254740992::bigint AS a;

-- begin-expected
-- columns: a
-- row: 9007199254740993
-- end-expected
SELECT greatest(9007199254740993::bigint, 9007199254740992::bigint) AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid > '7fffffff-ffff-ffff-ffff-ffffffffffff'::uuid AS a;

-- begin-expected
-- columns: u
-- row: 00000000-0000-0000-0000-000000000001
-- row: 7fffffff-ffff-ffff-ffff-ffffffffffff
-- row: 80000000-0000-0000-0000-000000000000
-- row: ffffffff-ffff-ffff-ffff-ffffffffffff
-- end-expected
SELECT u FROM (VALUES ('00000000-0000-0000-0000-000000000001'::uuid),('7fffffff-ffff-ffff-ffff-ffffffffffff'::uuid),('80000000-0000-0000-0000-000000000000'::uuid),('ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)) t(u) ORDER BY u;

CREATE TABLE zz_cmp_txt (t text);

INSERT INTO zz_cmp_txt VALUES ('a'),('a '),('a  '),('b');

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*) AS a FROM zz_cmp_txt WHERE t = 'a';

-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT count(*) AS a FROM zz_cmp_txt x JOIN zz_cmp_txt y ON x.t = y.t;

CREATE TABLE zz_cmp_big (b bigint);

INSERT INTO zz_cmp_big VALUES (9007199254740992),(9007199254740993);

-- begin-expected
-- columns: a
-- row: 9007199254740993
-- end-expected
SELECT max(b) AS a FROM zz_cmp_big;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*) AS a FROM zz_cmp_big WHERE b > 9007199254740992;

CREATE TABLE zz_cmp_md (n numeric);

INSERT INTO zz_cmp_md VALUES (1.0),(1.00),(1.000),(2.0),(2.0);

-- begin-expected
-- columns: a
-- row: 1.0
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY n) AS a FROM zz_cmp_md;

CREATE TYPE zz_cmp_e AS ENUM ('lo','hi');

CREATE TABLE zz_cmp_ec (a zz_cmp_e, b zz_cmp_e);

INSERT INTO zz_cmp_ec VALUES ('lo','hi');

-- begin-expected
-- columns: a
-- row: -1
-- end-expected
SELECT enum_cmp(a, b) AS a FROM zz_cmp_ec;

CREATE TABLE zz_cmp_f1 (a int, b int, PRIMARY KEY (a, b));

INSERT INTO zz_cmp_f1 VALUES (1, 1);

CREATE TABLE zz_cmp_f2 (x int, y int);

INSERT INTO zz_cmp_f2 VALUES (7, NULL);

ALTER TABLE zz_cmp_f2 ADD CONSTRAINT zz_cmp_f2_fk FOREIGN KEY (x, y) REFERENCES zz_cmp_f1 (a, b);

CREATE TABLE zz_cmp_n1 (a numeric PRIMARY KEY);

INSERT INTO zz_cmp_n1 VALUES (1.0);

CREATE TABLE zz_cmp_n2 (x numeric);

INSERT INTO zz_cmp_n2 VALUES (1.00);

ALTER TABLE zz_cmp_n2 ADD CONSTRAINT zz_cmp_n2_fk FOREIGN KEY (x) REFERENCES zz_cmp_n1 (a);

DROP TABLE zz_cmp_txt, zz_cmp_big, zz_cmp_md, zz_cmp_ec, zz_cmp_f2, zz_cmp_f1, zz_cmp_n2, zz_cmp_n1;

DROP TYPE zz_cmp_e;

