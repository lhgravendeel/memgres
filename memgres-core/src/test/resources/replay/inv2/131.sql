-- source: investigation-2026-08.md
-- finding: 131
-- title: Unrelated singletons in this area
-- extended protocol, not reproducible over pgjdbc:
-- P: SELECT $1  (paramOids = [23])
-- B: paramFormats=[1], params=[00 00 01]   (three bytes, not four)
-- E / S
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g (fid int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_f VALUES (1),(2),(3);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_g VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "g"
-- end-expected-error
SELECT count(*) FROM zz_f f, LATERAL zz_g g WHERE g.fid = f.id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_la (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lb (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_la VALUES (1),(2);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_lb VALUES (1),(2);
-- begin-expected
-- columns: id:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT j.id FROM zz_la JOIN zz_lb USING (id) AS j ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_z1 (a int, b text);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_z1 VALUES (1,'p'),(2,'q'),(3,'r');
-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator + is not a valid ordering operator
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING +;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_z1" already exists
-- end-expected-error
CREATE TABLE zz_z1 (a int, b text);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_z1 VALUES (1,'p'),(2,'q'),(3,'r');
-- begin-expected
-- columns: a:int4 | b:text
-- row: 1 | p
-- row: 1 | p
-- row: 2 | q
-- row: 2 | q
-- row: 3 | r
-- row: 3 | r
-- rowcount: 6
-- end-expected
TABLE ONLY zz_z1 ORDER BY a;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_z1" already exists
-- end-expected-error
CREATE TABLE zz_z1 (a int, b text);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_z1 VALUES (1,'p'),(2,'q'),(3,'r');
-- begin-expected
-- columns: count:int8
-- row: 9
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_z1 * a;
-- begin-expected
-- columns: 
-- row: 
-- rowcount: 1
-- end-expected
SELECT UNION SELECT;
-- begin-expected
-- columns: 
-- row: 
-- row: 
-- rowcount: 2
-- end-expected
SELECT UNION ALL SELECT;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: VALUES lists must all be the same length
-- end-expected-error
VALUES (1,2),(3);
-- begin-expected
-- columns: pg_typeof:text
-- row: text
-- rowcount: 1
-- end-expected
SELECT pg_typeof(column1)::text FROM (VALUES (NULL),(NULL)) v LIMIT 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = boolean
-- end-expected-error
SELECT 1 = 1 IN (1,2);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: boolean = integer
-- end-expected-error
SELECT (1 = 1) IN (1,2);
