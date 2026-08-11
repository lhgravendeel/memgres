CREATE TABLE zzc_colmsg (id int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" of relation "zzc_colmsg" does not exist
-- end-expected-error
ALTER TABLE zzc_colmsg ALTER COLUMN nosuchcol SET NOT NULL;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" does not exist
-- end-expected-error
ALTER TABLE zzc_colmsg RENAME COLUMN nosuchcol TO x;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "ctid" of relation "zzc_colmsg" does not exist
-- end-expected-error
INSERT INTO zzc_colmsg (ctid) VALUES ('(0,1)');

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "oid" of relation "zzc_colmsg" does not exist
-- end-expected-error
INSERT INTO zzc_colmsg (oid) VALUES (1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot assign to system column "ctid"
-- end-expected-error
UPDATE zzc_colmsg SET ctid = '(0,1)';

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO zzc_colmsg VALUES (1) RETURNING nosuchcol;

DROP TABLE zzc_colmsg;

CREATE TABLE zzc_r5 (i int primary key, v text);
CREATE VIEW zzc_r5v AS SELECT i, v FROM zzc_r5;
CREATE RULE zzc_r5_r AS ON INSERT TO zzc_r5v DO INSTEAD INSERT INTO zzc_r5 VALUES (NEW.i, NEW.v);
CREATE RULE zzc_r5_u AS ON UPDATE TO zzc_r5v DO INSTEAD UPDATE zzc_r5 SET v = NEW.v WHERE i = OLD.i;
CREATE RULE zzc_r5_d AS ON DELETE TO zzc_r5v DO INSTEAD DELETE FROM zzc_r5 WHERE i = OLD.i;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot perform INSERT RETURNING on relation "zzc_r5v"
-- end-expected-error
INSERT INTO zzc_r5v VALUES (1,'a') RETURNING i;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzc_r5;

INSERT INTO zzc_r5v VALUES (1,'a');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot perform UPDATE RETURNING on relation "zzc_r5v"
-- end-expected-error
UPDATE zzc_r5v SET v='b' WHERE i=1 RETURNING i;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot perform DELETE RETURNING on relation "zzc_r5v"
-- end-expected-error
DELETE FROM zzc_r5v WHERE i=1 RETURNING i;

DROP VIEW zzc_r5v;
DROP TABLE zzc_r5;

CREATE TABLE zzc_v3t (i int, n int);
CREATE VIEW zzc_v3v AS SELECT i, n*2 AS dn FROM zzc_v3t;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot insert into column "dn" of view "zzc_v3v"
-- end-expected-error
INSERT INTO zzc_v3v VALUES (1, 4);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzc_v3t;

INSERT INTO zzc_v3v VALUES (1);

-- begin-expected
-- columns: i,n
-- row: 1 | NULL
-- end-expected
SELECT i, n FROM zzc_v3t;

DROP VIEW zzc_v3v;
DROP TABLE zzc_v3t;

CREATE DOMAIN zzc_wd AS int DEFAULT 7;
CREATE TABLE zzc_wt (a int);
INSERT INTO zzc_wt VALUES (1),(2);
ALTER TABLE zzc_wt ADD COLUMN b zzc_wd;

-- begin-expected
-- columns: a,b
-- row: 1, 7
-- row: 2, 7
-- end-expected
SELECT a, b FROM zzc_wt ORDER BY a;

-- begin-expected
-- columns: column_default
-- row: 
-- end-expected
SELECT column_default FROM information_schema.columns WHERE table_name='zzc_wt' AND column_name='b';

DROP TABLE zzc_wt;
DROP DOMAIN zzc_wd;

CREATE TABLE zzc_fta (shared int, a int);
CREATE TABLE zzc_ftb (shared int NOT NULL, b int);
CREATE TABLE zzc_ftc () INHERITS (zzc_fta, zzc_ftb);

-- begin-expected
-- columns: is_nullable
-- row: NO
-- end-expected
SELECT is_nullable FROM information_schema.columns WHERE table_name='zzc_ftc' AND column_name='shared';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: ERROR: null value in column "shared" of relation "zzc_ftc" violates not-null constraint
-- end-expected-error
INSERT INTO zzc_ftc (a) VALUES (1);

DROP TABLE zzc_ftc;
DROP TABLE zzc_fta;
DROP TABLE zzc_ftb;

CREATE TABLE zzc_fka (x int CONSTRAINT zzc_k CHECK (x > 0));
CREATE TABLE zzc_fkb (x int CONSTRAINT zzc_k CHECK (x < 0));

-- begin-expected-error
-- sqlstate: 42710
-- message-like: ERROR: check constraint name "zzc_k" appears multiple times but with different expressions
-- end-expected-error
CREATE TABLE zzc_fkc () INHERITS (zzc_fka, zzc_fkb);

DROP TABLE zzc_fka;
DROP TABLE zzc_fkb;

CREATE TYPE zzc_e AS ENUM ('lo','mid','hi');
CREATE TABLE zzc_pe (e zzc_e NOT NULL) PARTITION BY RANGE (e);
CREATE TABLE zzc_pe1 PARTITION OF zzc_pe FOR VALUES FROM ('lo') TO ('hi');
INSERT INTO zzc_pe VALUES ('mid');

-- begin-expected
-- columns: e
-- row: mid
-- end-expected
SELECT e FROM zzc_pe;

-- begin-expected
-- columns: pg_get_expr
-- row: FOR VALUES FROM ('lo') TO ('hi')
-- end-expected
SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname='zzc_pe1';

DROP TABLE zzc_pe;
DROP TYPE zzc_e;

CREATE DOMAIN zzc_dp AS int CHECK (VALUE > 0);
CREATE TABLE zzc_pd (k zzc_dp NOT NULL) PARTITION BY RANGE (k);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: value for domain zzc_dp violates check constraint "zzc_dp_check"
-- end-expected-error
CREATE TABLE zzc_pd1 PARTITION OF zzc_pd FOR VALUES FROM (-100) TO (0);

DROP TABLE zzc_pd;
DROP DOMAIN zzc_dp;

CREATE VIEW zzc_sv WITH (security_barrier) AS SELECT 1 AS x;

-- begin-expected
-- columns: reloptions
-- row: {security_barrier=true}
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='zzc_sv';

DROP VIEW zzc_sv;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: WITH CHECK OPTION is supported only on automatically updatable views
-- end-expected-error
CREATE VIEW zzc_sv4 WITH (check_option = cascaded) AS SELECT 1 AS x;

CREATE TABLE zzc_tp (a int);
CREATE TABLE zzc_tc () INHERITS (zzc_tp);
INSERT INTO zzc_tp VALUES (1);
INSERT INTO zzc_tc VALUES (2);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzc_tp TABLESAMPLE BERNOULLI (100);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzc_tp;

DROP TABLE zzc_tc;
DROP TABLE zzc_tp;