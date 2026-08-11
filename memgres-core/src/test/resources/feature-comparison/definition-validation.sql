CREATE TABLE zzc1h_d6 (a int);
INSERT INTO zzc1h_d6 VALUES (1);

-- begin-expected-error
-- sqlstate: 22001
-- message-like: ERROR: value too long for type character varying(3)
-- end-expected-error
ALTER TABLE zzc1h_d6 ADD COLUMN s varchar(3) DEFAULT 'random(9)';

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "s" does not exist
-- end-expected-error
SELECT a, s FROM zzc1h_d6;

DROP TABLE zzc1h_d6;

CREATE TABLE zzc4h_t1 (keepme int, e int);
CREATE VIEW zzc4h_v1 AS SELECT keepme FROM zzc4h_t1;

ALTER TABLE zzc4h_t1 DROP COLUMN e;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM information_schema.columns WHERE table_name='zzc4h_t1';

DROP VIEW zzc4h_v1;
DROP TABLE zzc4h_t1;

CREATE TABLE zzc5h_dv (i int, v text);
CREATE VIEW zzc5h_dvv AS SELECT i, v FROM zzc5h_dv;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: ERROR: cannot drop column v of table zzc5h_dv because other objects depend on it
-- end-expected-error
ALTER TABLE zzc5h_dv DROP COLUMN v;

ALTER TABLE zzc5h_dv DROP COLUMN v CASCADE;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname='zzc5h_dvv';

DROP TABLE zzc5h_dv;

CREATE TABLE zzc8h_id1 (i int GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY -10 MINVALUE 1 MAXVALUE 1000), j int);
INSERT INTO zzc8h_id1 (j) VALUES (1);
INSERT INTO zzc8h_id1 (j) VALUES (2);

-- begin-expected
-- columns: i, j
-- row: 100, 1
-- row: 90, 2
-- end-expected
SELECT i, j FROM zzc8h_id1 ORDER BY j;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: INCREMENT must not be zero
-- end-expected-error
CREATE TABLE zzc8h_i3 (i int GENERATED ALWAYS AS IDENTITY (INCREMENT BY 0), j int);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: START value (-5) cannot be less than MINVALUE (1)
-- end-expected-error
CREATE TABLE zzc8h_i4 (i int GENERATED ALWAYS AS IDENTITY (START WITH -5), j int);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type bigint: "1.5"
-- end-expected-error
CREATE TABLE zzc8h_i5 (i int GENERATED ALWAYS AS IDENTITY (START WITH 1.5), j int);

DROP TABLE zzc8h_id1;

CREATE TABLE zzcbh_r3 (a int, b int);

-- begin-expected
-- columns: attname, attstattarget
-- row: a, null
-- row: b, null
-- end-expected
SELECT attname, attstattarget FROM pg_attribute WHERE attrelid='zzcbh_r3'::regclass AND attnum>0 ORDER BY attnum;

ALTER TABLE zzcbh_r3 ALTER COLUMN a SET STATISTICS 100000;

-- begin-expected
-- columns: attname, attstattarget
-- row: a, 10000
-- row: b, null
-- end-expected
SELECT attname, attstattarget FROM pg_attribute WHERE attrelid='zzcbh_r3'::regclass AND attnum>0 ORDER BY attnum;

ALTER TABLE zzcbh_r3 ALTER COLUMN a SET STATISTICS -1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "2147483648"
-- end-expected-error
ALTER TABLE zzcbh_r3 ALTER COLUMN a SET STATISTICS 2147483648;

DROP TABLE zzcbh_r3;

CREATE TABLE zzcdh_g1 (a int, c int GENERATED ALWAYS AS (a+1) STORED);
INSERT INTO zzcdh_g1 (a) VALUES (5);

ALTER TABLE zzcdh_g1 ALTER COLUMN c SET EXPRESSION AS (a*10);

-- begin-expected
-- columns: a, c
-- row: 5, 50
-- end-expected
SELECT a, c FROM zzcdh_g1;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: column "a" of relation "zzcdh_g1" is not a generated column
-- end-expected-error
ALTER TABLE zzcdh_g1 ALTER COLUMN a SET EXPRESSION AS (c*2);

DROP TABLE zzcdh_g1;

CREATE TABLE zzcfh_o1 (a int);

ALTER TABLE zzcfh_o1 ALTER COLUMN a SET (n_distinct = 5);

ALTER TABLE zzcfh_o1 ALTER COLUMN a RESET (n_distinct);

CREATE TABLE zzcfh_inc (a int, b int, UNIQUE (a) INCLUDE (b));

CREATE TABLE zzcfh_inc2 (a int, b int, PRIMARY KEY (a) INCLUDE (b));

CREATE TABLE zzcfh_nn (a int NOT NULL NO INHERIT);

DROP TABLE zzcfh_o1;
DROP TABLE zzcfh_inc;
DROP TABLE zzcfh_inc2;
DROP TABLE zzcfh_nn;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" named in key does not exist
-- end-expected-error
CREATE TABLE zzcjh_u1 (a int, UNIQUE (nosuchcol));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" named in key does not exist
-- end-expected-error
CREATE TABLE zzcjh_u4 (a int, PRIMARY KEY (nosuchcol));

-- begin-expected-error
-- sqlstate: 42701
-- message-like: ERROR: column "a" appears twice in primary key constraint
-- end-expected-error
CREATE TABLE zzcjh_u2 (a int, PRIMARY KEY (a, a));

-- begin-expected-error
-- sqlstate: 42701
-- message-like: ERROR: column "a" appears twice in unique constraint
-- end-expected-error
CREATE TABLE zzcjh_u5 (a int, b int, UNIQUE (a, a));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: access method "gin" does not support exclusion constraints
-- end-expected-error
CREATE TABLE zzcjh_u3 (a int, EXCLUDE USING gin (a WITH =));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: multiple default values specified for column "a" of table "zzckh_cf1"
-- end-expected-error
CREATE TABLE zzckh_cf1 (a int DEFAULT 1 DEFAULT 2);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: conflicting NULL/NOT NULL declarations for column "a" of table "zzckh_cf2"
-- end-expected-error
CREATE TABLE zzckh_cf2 (a int NOT NULL NULL);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: multiple default values specified for column "i" of table "zzckh_cf3"
-- end-expected-error
CREATE TABLE zzckh_cf3 (i serial DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: both identity and generation expression specified for column "b" of table "zzckh_cf4"
-- end-expected-error
CREATE TABLE zzckh_cf4 (a int, b int GENERATED ALWAYS AS (a) STORED GENERATED ALWAYS AS IDENTITY);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: for a generated column, GENERATED ALWAYS must be specified
-- end-expected-error
CREATE TABLE zzckh_gc3 (a int, b int GENERATED BY DEFAULT AS (a) STORED);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: ERROR: cannot use "list" partition strategy with more than one column
-- end-expected-error
CREATE TABLE zzcnh_p1 (a int, b int) PARTITION BY LIST (a, b);

CREATE TABLE zzcnh_p2 (a int, b int) PARTITION BY RANGE (a);
CREATE TABLE zzcnh_p2_1 PARTITION OF zzcnh_p2 FOR VALUES FROM (1) TO (10);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: cannot add column to a partition
-- end-expected-error
ALTER TABLE zzcnh_p2_1 ADD COLUMN c int;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: ERROR: cannot create temporary relation in non-temporary schema
-- end-expected-error
CREATE TEMP TABLE public.zzcnh_tt (a int);

CREATE TABLE zzcnh_misc (a int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ","
-- end-expected-error
ALTER TABLE zzcnh_misc RENAME COLUMN a TO c, ADD COLUMN d int;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: type "serial" does not exist
-- end-expected-error
ALTER TABLE zzcnh_misc ALTER COLUMN a TYPE serial;

DROP TABLE zzcnh_p2;
DROP TABLE zzcnh_misc;

CREATE TABLE zzcqh_k1 (a int);
INSERT INTO zzcqh_k1 VALUES (0);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: ERROR: division by zero
-- end-expected-error
ALTER TABLE zzcqh_k1 ADD CONSTRAINT zzcqh_k1_ck CHECK (100 / a > 0);

CREATE TABLE zzcqh_nv (a int);
INSERT INTO zzcqh_nv VALUES (NULL);

ALTER TABLE zzcqh_nv ADD CONSTRAINT zzcqh_nn1 NOT NULL a NOT VALID;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzcqh_ck1 (a int CHECK (nosuchcol > 0));

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ERROR: system column "xmin" reference in check constraint is invalid
-- end-expected-error
CREATE TABLE zzcqh_ck2 (a int CHECK (xmin > 0));

-- begin-expected-error
-- sqlstate: 42710
-- message-like: ERROR: check constraint "zzcqh_c1" already exists
-- end-expected-error
CREATE TABLE zzcqh_ck4 (a int, CONSTRAINT zzcqh_c1 CHECK (a>0), CONSTRAINT zzcqh_c1 CHECK (a<9));

CREATE TABLE zzcqh_rc (id int CONSTRAINT zzcqh_pk1 PRIMARY KEY);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: constraint "zzcqh_nosuch" for table "zzcqh_rc" does not exist
-- end-expected-error
ALTER TABLE zzcqh_rc RENAME CONSTRAINT zzcqh_nosuch TO x;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: constraint "zzcqh_nosuch" of relation "zzcqh_rc" does not exist
-- end-expected-error
ALTER TABLE zzcqh_rc DROP CONSTRAINT zzcqh_nosuch;

DROP TABLE zzcqh_k1;
DROP TABLE zzcqh_nv;
DROP TABLE zzcqh_rc;

CREATE SEQUENCE zzcth_sqt;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: type "bogustype" does not exist
-- end-expected-error
ALTER SEQUENCE zzcth_sqt AS bogustype;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: sequence type must be smallint, integer, or bigint
-- end-expected-error
ALTER SEQUENCE zzcth_sqt AS text;

CREATE TABLE zzcth_orv (i int, v text);
CREATE VIEW zzcth_orvv AS SELECT i, v FROM zzcth_orv;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: ERROR: cannot change data type of view column "v" from text to integer
-- end-expected-error
CREATE OR REPLACE VIEW zzcth_orvv AS SELECT i, i AS v FROM zzcth_orv;

CREATE FUNCTION zzcth_trgf() RETURNS trigger AS $$ BEGIN RETURN NULL; END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: "zzcth_orvv" is a view
-- end-expected-error
CREATE TRIGGER zzcth_trg INSTEAD OF TRUNCATE ON zzcth_orvv FOR EACH STATEMENT EXECUTE FUNCTION zzcth_trgf();

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: ALTER action ADD COLUMN cannot be performed on relation "zzcth_orvv"
-- end-expected-error
ALTER VIEW zzcth_orvv ADD COLUMN q int;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "SUCH"
-- end-expected-error
ALTER VIEW zzcth_orvv NO SUCH ACTION;

DROP FUNCTION zzcth_trgf();
DROP VIEW zzcth_orvv;
DROP TABLE zzcth_orv;
DROP SEQUENCE zzcth_sqt;