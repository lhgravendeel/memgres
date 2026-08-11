CREATE TABLE zzg1c_a1 (a int);
ALTER TABLE zzg1c_a1 ADD COLUMN b int UNIQUE;
INSERT INTO zzg1c_a1 (b) VALUES (1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: ERROR: duplicate key value violates unique constraint "zzg1c_a1_b_key"
-- end-expected-error
INSERT INTO zzg1c_a1 (b) VALUES (1);

-- begin-expected
-- columns: conname, contype
-- row: zzg1c_a1_b_key, u
-- end-expected
SELECT conname, contype FROM pg_constraint WHERE conrelid='zzg1c_a1'::regclass AND contype='u';

CREATE TABLE zzg1c_a5 (a int);
ALTER TABLE zzg1c_a5 ADD COLUMN b int CONSTRAINT zzg1c_myuq UNIQUE;

-- begin-expected
-- columns: conname, contype
-- row: zzg1c_myuq, u
-- end-expected
SELECT conname, contype FROM pg_constraint WHERE conrelid='zzg1c_a5'::regclass AND contype='u';

DROP TABLE zzg1c_a1;
DROP TABLE zzg1c_a5;

CREATE TABLE zzg1c_a6 (a int);
INSERT INTO zzg1c_a6 VALUES (1);
INSERT INTO zzg1c_a6 VALUES (2);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: ERROR: could not create unique index "zzg1c_a6_b_key"
-- end-expected-error
ALTER TABLE zzg1c_a6 ADD COLUMN b int UNIQUE DEFAULT 7;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "b" does not exist
-- end-expected-error
SELECT a, b FROM zzg1c_a6 ORDER BY a;

DROP TABLE zzg1c_a6;

CREATE TABLE zzg1c_f6 (i int) PARTITION BY RANGE (i);
CREATE TABLE zzg1c_f6_1 PARTITION OF zzg1c_f6 FOR VALUES FROM (1) TO (10);
ALTER TABLE zzg1c_f6_1 RENAME TO zzg1c_f6_x;
INSERT INTO zzg1c_f6 VALUES (5);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM zzg1c_f6_x;

-- begin-expected
-- columns: relname
-- row: zzg1c_f6_x
-- end-expected
SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid WHERE i.inhparent='zzg1c_f6'::regclass;

DROP TABLE zzg1c_f6 CASCADE;

CREATE UNLOGGED TABLE zzg1c_u1 (a int PRIMARY KEY);
ALTER TABLE zzg1c_u1 REPLICA IDENTITY FULL;
ALTER TABLE zzg1c_u1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE zzg1c_u1 FORCE ROW LEVEL SECURITY;
ALTER TABLE zzg1c_u1 RENAME TO zzg1c_u2;

-- begin-expected
-- columns: relpersistence, relreplident, relrowsecurity, relforcerowsecurity
-- row: u, f, true, true
-- end-expected
SELECT relpersistence, relreplident, relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname='zzg1c_u2';

CREATE TABLE zzg1c_r1 (a int) WITH (fillfactor=70);
ALTER TABLE zzg1c_r1 RENAME TO zzg1c_r2;

-- begin-expected
-- columns: reloptions
-- row: {fillfactor=70}
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='zzg1c_r2';

CREATE TABLE zzg1c_r3 (a int);
ALTER TABLE zzg1c_r3 ENABLE ROW LEVEL SECURITY;
CREATE POLICY zzg1c_p1 ON zzg1c_r3 USING (a > 0);
ALTER TABLE zzg1c_r3 RENAME TO zzg1c_r4;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_policies WHERE tablename='zzg1c_r4';

DROP TABLE zzg1c_u2;
DROP TABLE zzg1c_r2;
DROP TABLE zzg1c_r4;

CREATE TABLE zzg1c_r5 (i serial, v text);
INSERT INTO zzg1c_r5 (v) VALUES ('a');
ALTER TABLE zzg1c_r5 RENAME TO zzg1c_r6;

-- begin-expected
-- columns: relname
-- row: zzg1c_r5_i_seq
-- end-expected
SELECT relname FROM pg_class WHERE relkind='S' AND relname LIKE 'zzg1c_r%' ORDER BY 1;

CREATE TABLE zzg1c_z1 (a int GENERATED ALWAYS AS IDENTITY, b int) PARTITION BY RANGE (b);
CREATE TABLE zzg1c_z1p PARTITION OF zzg1c_z1 FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: relname
-- row: zzg1c_z1_a_seq
-- end-expected
SELECT relname FROM pg_class WHERE relkind='S' AND relname LIKE 'zzg1c_z1%' ORDER BY 1;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_sequences WHERE sequencename LIKE 'zzg1c_z1%';

DROP TABLE zzg1c_r6 CASCADE;
DROP TABLE zzg1c_z1 CASCADE;

CREATE TABLE zzg1c_i1 (i int GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 2 CYCLE), j int);
INSERT INTO zzg1c_i1 (j) VALUES (1),(2),(3);

-- begin-expected
-- columns: i, j
-- row: 1, 1
-- row: 2, 2
-- row: 1, 3
-- end-expected
SELECT i, j FROM zzg1c_i1 ORDER BY j;

-- begin-expected
-- columns: sequencename, start_value, min_value, max_value, increment_by, cycle, cache_size
-- row: zzg1c_i1_i_seq, 1, 1, 2, 1, true, 1
-- end-expected
SELECT sequencename, start_value, min_value, max_value, increment_by, cycle, cache_size FROM pg_sequences WHERE sequencename LIKE 'zzg1c_i1%';

CREATE TABLE zzg1c_i3 (i int GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME zzg1c_myseq), j int);

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: public.zzg1c_myseq
-- end-expected
SELECT pg_get_serial_sequence('zzg1c_i3','i');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: CACHE (0) must be greater than zero
-- end-expected-error
CREATE TABLE zzg1c_i6 (i int GENERATED ALWAYS AS IDENTITY (CACHE 0), j int);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: MINVALUE (5) must be less than MAXVALUE (3)
-- end-expected-error
CREATE TABLE zzg1c_i7 (i int GENERATED ALWAYS AS IDENTITY (MINVALUE 5 MAXVALUE 3), j int);

CREATE TABLE zzg1c_i9 (i int GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY -10 MINVALUE 1 MAXVALUE 1000), j int);
INSERT INTO zzg1c_i9 (j) VALUES (1),(2);

-- begin-expected
-- columns: i, j
-- row: 100, 1
-- row: 90, 2
-- end-expected
SELECT i, j FROM zzg1c_i9 ORDER BY j;

DROP TABLE zzg1c_i1 CASCADE;
DROP TABLE zzg1c_i3 CASCADE;
DROP TABLE zzg1c_i9 CASCADE;

CREATE TABLE zzg1c_i4 (a int GENERATED BY DEFAULT AS IDENTITY, b text);
ALTER TABLE zzg1c_i4 ALTER COLUMN a SET INCREMENT BY 10;
INSERT INTO zzg1c_i4 (b) VALUES ('p');

-- begin-expected
-- columns: a
-- row: 11
-- end-expected
INSERT INTO zzg1c_i4 (b) VALUES ('q') RETURNING a;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: column "a" of relation "zzg1c_i4" is an identity column
-- end-expected-error
ALTER TABLE zzg1c_i4 ALTER COLUMN a DROP NOT NULL;

CREATE TABLE zzg1c_j2 (a int, b serial);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: column "a" of relation "zzg1c_j2" is not an identity column
-- end-expected-error
ALTER TABLE zzg1c_j2 ALTER COLUMN a SET INCREMENT BY 5;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: column "b" of relation "zzg1c_j2" is not an identity column
-- end-expected-error
ALTER TABLE zzg1c_j2 ALTER COLUMN b SET INCREMENT BY 5;

DROP TABLE zzg1c_i4 CASCADE;
DROP TABLE zzg1c_j2 CASCADE;

CREATE TABLE zzg1c_o1 (i serial, v text);
ALTER TABLE zzg1c_o1 DROP COLUMN i;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname='zzg1c_o1_i_seq';

CREATE SEQUENCE zzg1c_o1_i_seq;

CREATE TABLE zzg1c_o2 (id int GENERATED ALWAYS AS IDENTITY, v text);
ALTER TABLE zzg1c_o2 ALTER COLUMN id DROP IDENTITY;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname='zzg1c_o2_id_seq';

CREATE TABLE zzg1c_w1 (a int, b text);
CREATE SEQUENCE zzg1c_w1s;
ALTER SEQUENCE zzg1c_w1s OWNED BY zzg1c_w1.a;
DROP TABLE zzg1c_w1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname='zzg1c_w1s';

CREATE TABLE zzg1c_w2 (i serial, v text);
ALTER TABLE zzg1c_w2 RENAME COLUMN i TO j;
DROP TABLE zzg1c_w2 CASCADE;

-- begin-expected
-- columns: relname
-- end-expected
SELECT relname FROM pg_class WHERE relname LIKE 'zzg1c_w2%';

DROP TABLE zzg1c_o1;
DROP TABLE zzg1c_o2;
DROP SEQUENCE zzg1c_o1_i_seq;

CREATE SEQUENCE zzg1c_ds;
CREATE TABLE zzg1c_o3 (a int DEFAULT nextval('zzg1c_ds'), b text);

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: NULL
-- end-expected
SELECT pg_get_serial_sequence('zzg1c_o3','a');

CREATE TABLE zzg1c_o4 (i serial, v text);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "I" of relation "zzg1c_o4" does not exist
-- end-expected-error
SELECT pg_get_serial_sequence('zzg1c_o4','I');

CREATE TABLE zzg1c_q5 ("Cap" serial, v text);

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: public."zzg1c_q5_Cap_seq"
-- end-expected
SELECT pg_get_serial_sequence('zzg1c_q5','Cap');

DROP TABLE zzg1c_o3;
DROP TABLE zzg1c_o4 CASCADE;
DROP TABLE zzg1c_q5 CASCADE;
DROP SEQUENCE zzg1c_ds;

CREATE SEQUENCE zzg1c_s1;
SELECT nextval('zzg1c_s1');
DROP SEQUENCE zzg1c_s1;
CREATE SEQUENCE zzg1c_s1;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: currval of sequence "zzg1c_s1" is not yet defined in this session
-- end-expected-error
SELECT currval('zzg1c_s1');

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: lastval is not yet defined in this session
-- end-expected-error
SELECT lastval();

CREATE SEQUENCE zzg1c_t1;
SELECT nextval('zzg1c_t1');
ALTER SEQUENCE zzg1c_t1 RENAME TO zzg1c_t2;

-- begin-expected
-- columns: currval
-- row: 1
-- end-expected
SELECT currval('zzg1c_t2');

CREATE SEQUENCE zzg1c_s6 CACHE 5 CYCLE;
SELECT nextval('zzg1c_s6');

-- begin-expected
-- columns: last_value, is_called
-- row: 5, true
-- end-expected
SELECT last_value, is_called FROM zzg1c_s6;

CREATE SEQUENCE zzg1c_s3 CACHE 7;

-- begin-expected
-- columns: seqcache
-- row: 7
-- end-expected
SELECT seqcache FROM pg_sequence WHERE seqrelid='zzg1c_s3'::regclass;

CREATE SEQUENCE zzg1c_q9 AS integer;

-- begin-expected
-- columns: seqtypid, seqmax
-- row: integer, 2147483647
-- end-expected
SELECT seqtypid::regtype::text AS seqtypid, seqmax FROM pg_sequence WHERE seqrelid='zzg1c_q9'::regclass;

CREATE UNLOGGED SEQUENCE zzg1c_s4;

-- begin-expected
-- columns: relpersistence
-- row: u
-- end-expected
SELECT relpersistence FROM pg_class WHERE relname='zzg1c_s4';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: conflicting or redundant options
-- end-expected-error
CREATE SEQUENCE zzg1c_s8 START 1 START 2;

CREATE SEQUENCE zzg1c_s9;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at end of input
-- end-expected-error
ALTER SEQUENCE zzg1c_s9;

DROP SEQUENCE zzg1c_s1;
DROP SEQUENCE zzg1c_t2;
DROP SEQUENCE zzg1c_s6;
DROP SEQUENCE zzg1c_s3;
DROP SEQUENCE zzg1c_q9;
DROP SEQUENCE zzg1c_s4;
DROP SEQUENCE zzg1c_s9;