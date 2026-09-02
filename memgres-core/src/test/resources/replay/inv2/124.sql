-- source: investigation-2026-08.md
-- finding: 124
-- title: CREATE INDEX registration: the whole block is skipped when no name was written, expression keys are evaluated rather than resolved, keys are joined with a bare 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ct (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX ON zz_ct (a);
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_indexes WHERE tablename = 'zz_ct';
-- begin-expected
-- ok: 0
-- end-expected
DROP INDEX zz_ct_a_idx;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_cp (a int, b int) PARTITION BY RANGE (a);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_cpa PARTITION OF zz_cp FOR VALUES FROM (0) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_c8 ON zz_cp (b);
-- begin-expected
-- columns: indexname:name
-- row: zz_cpa_b_idx
-- rowcount: 1
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zz_cpa';
-- begin-expected
-- ok: 0
-- end-expected
DROP INDEX zz_c8;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_indexes WHERE tablename = 'zz_cpa';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_h1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_h1.ht2 (id int);
-- begin-expected
-- ok: 0
-- end-expected
SET search_path TO public, zz_h1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_hidx2 ON ht2 (id);
-- begin-expected
-- columns: schemaname:name | indexname:name
-- row: zz_h1 | zz_hidx2
-- rowcount: 1
-- end-expected
SELECT schemaname, indexname FROM pg_indexes WHERE indexname = 'zz_hidx2';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_n3 (a text, b text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_n3 VALUES ('a' || chr(1) || 'b', 'c'), ('a', 'b' || chr(1) || 'c');
-- begin-expected
-- ok: 0
-- end-expected
CREATE UNIQUE INDEX zz_i3 ON zz_n3 (a, b);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_ct" already exists
-- end-expected-error
CREATE TABLE zz_ct (a int, b text);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE INDEX zz_c1 ON zz_ct ((a + nosuchcol));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_n2 (v int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_n2 VALUES (NULL), (NULL);
-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zz_i2"
-- end-expected-error
CREATE UNIQUE INDEX zz_i2 ON zz_n2 (v) NULLS NOT DISTINCT;
