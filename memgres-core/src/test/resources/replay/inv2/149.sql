-- source: investigation-2026-08.md
-- finding: 149
-- title: Numeric option values are read with Long.parseLong / Integer.parseInt straight off a raw token, so a leading MINUS token, a value at the long boundary or a non-
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i int GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY -10 MINVALUE 1 MAXVALUE 1000), j int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ts (a int);
-- begin-expected
-- ok: 20
-- end-expected
INSERT INTO zz_ts SELECT g FROM generate_series(1,20) g;
-- begin-expected
-- columns: count:int8
-- row: 20
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_ts TABLESAMPLE BERNOULLI (100) REPEATABLE (1.5);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_s1 ALTER COLUMN a SET STATISTICS 100000;
-- begin-expected
-- columns: attstattarget:int2
-- row: 10000
-- rowcount: 1
-- end-expected
SELECT attstattarget FROM pg_attribute WHERE attrelid='zz_s1'::regclass AND attname='a';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s MINVALUE -9223372036854775808;
