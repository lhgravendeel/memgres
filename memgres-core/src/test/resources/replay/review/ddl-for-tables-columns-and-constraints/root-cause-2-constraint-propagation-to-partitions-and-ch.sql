-- source: review-2026-08.md
-- finding: Root cause 2: constraint propagation to partitions and children is restricted to PRIMARY KEY and UNIQUE
-- area: DDL for tables, columns and constraints
-- title: Root cause 2: constraint propagation to partitions and children is restricted to PRIMARY KEY and UNIQUE
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (a int, b text) PARTITION BY RANGE (a);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p_p0 PARTITION OF zz_p FOR VALUES FROM (0) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_p ADD CONSTRAINT zz_ck CHECK (b <> 'bad');
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zz_p_p0" violates check constraint "zz_ck"
-- end-expected-error
INSERT INTO zz_p_p0 VALUES (1, 'bad');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_par (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_chi () INHERITS (zz_par);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_par ADD CONSTRAINT zz_ck CHECK (a > 0);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zz_chi" violates check constraint "zz_ck"
-- end-expected-error
INSERT INTO zz_chi VALUES (-1);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_p" already exists
-- end-expected-error
CREATE TABLE zz_p (a int, b text) PARTITION BY RANGE (a);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_p_p0" already exists
-- end-expected-error
CREATE TABLE zz_p_p0 PARTITION OF zz_p FOR VALUES FROM (0) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_p ALTER COLUMN b SET NOT NULL;
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "b" of relation "zz_p_p0" violates not-null constraint
-- end-expected-error
INSERT INTO zz_p_p0 (a) VALUES (1);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_par" already exists
-- end-expected-error
CREATE TABLE zz_par (a int, b text);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_chi" already exists
-- end-expected-error
CREATE TABLE zz_chi () INHERITS (zz_par);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "b" of relation "zz_par" does not exist
-- end-expected-error
ALTER TABLE zz_par ALTER COLUMN b SET NOT NULL;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_chi (a) VALUES (1);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_par" already exists
-- end-expected-error
CREATE TABLE zz_par (a int, b text);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_chi" already exists
-- end-expected-error
CREATE TABLE zz_chi () INHERITS (zz_par);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "b" of relation "zz_par" does not exist
-- end-expected-error
ALTER TABLE zz_par ALTER COLUMN b SET DEFAULT 'q';
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_chi (a) VALUES (1);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "b" does not exist
-- end-expected-error
SELECT a, b FROM zz_chi;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_par" already exists
-- end-expected-error
CREATE TABLE zz_par (a int);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_chi" already exists
-- end-expected-error
CREATE TABLE zz_chi () INHERITS (zz_par);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_chi VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
TRUNCATE zz_par;
-- begin-expected
-- columns: n:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) AS n FROM zz_chi;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_gdp (a int CHECK (a > 0));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_gdc (a int);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table is missing constraint "zz_gdp_a_check"
-- end-expected-error
ALTER TABLE zz_gdc INHERIT zz_gdp;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ck2 (i int, v int) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ck2_1 PARTITION OF zz_ck2 FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_ck2 ADD CONSTRAINT zz_ck2_c CHECK (v > 0);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zz_ck2_1" violates check constraint "zz_ck2_c"
-- end-expected-error
INSERT INTO zz_ck2 VALUES (1, -1);
