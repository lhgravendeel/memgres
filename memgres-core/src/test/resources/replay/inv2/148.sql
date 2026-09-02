-- source: investigation-2026-08.md
-- finding: 148
-- title: Identifiers and expressions are matched as substrings of SQL text rather than resolved: a view dependency is `viewSql.contains(colName)`, a volatile default is 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d6 (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_d6 VALUES (1);
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
ALTER TABLE zz_d6 ADD COLUMN s varchar(3) DEFAULT 'random(9)';
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "s" does not exist
-- end-expected-error
SELECT a, s FROM zz_d6;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_sch;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_src (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_dup AS SELECT a FROM zz_src;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_sch.zz_dup (b int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_sch.zz_dup VALUES (7);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_sch.zz_dup RENAME TO zz_dup2;
-- begin-expected
-- columns: b:int4
-- row: 7
-- rowcount: 1
-- end-expected
SELECT b FROM zz_sch.zz_dup2;
-- begin-expected
-- columns: a:int4
-- rowcount: 0
-- end-expected
SELECT a FROM public.zz_dup;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (keepme int, e int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v AS SELECT keepme FROM zz_t;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t DROP COLUMN e;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s6;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s6.zz_ts (a int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_s6.zz_ts VALUES (1),(2),(3);
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_s6.zz_ts TABLESAMPLE BERNOULLI (100);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_NOSUCH" does not exist
-- end-expected-error
SELECT nextval('"zz_NOSUCH"');
