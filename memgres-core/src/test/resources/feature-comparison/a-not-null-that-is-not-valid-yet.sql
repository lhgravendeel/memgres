-- ============================================================================
-- A NOT NULL declared NOT VALID records itself on the relation and on every
-- child, and the rule is in force from the moment it is declared
--
-- NOT VALID defers the rows already there, never the rule itself: the column is
-- marked NOT NULL either way, the rows that were there may stay, and every row
-- written from here on is held to it.
-- ============================================================================
CREATE TABLE zze6gd_np (i int, j int);
CREATE TABLE zze6gd_nc () INHERITS (zze6gd_np);
INSERT INTO zze6gd_np VALUES (1, NULL);
INSERT INTO zze6gd_nc VALUES (2, NULL);

ALTER TABLE zze6gd_np ADD CONSTRAINT zze6gd_nn NOT NULL j NOT VALID;

-- begin-expected
-- columns: relname | conname | contype | convalidated | conislocal | coninhcount | connoinherit | conenforced
-- row: zze6gd_nc | zze6gd_nn | n | f | f | 1 | f | t
-- row: zze6gd_np | zze6gd_nn | n | f | t | 0 | f | t
-- end-expected
SELECT c.relname, o.conname, o.contype, o.convalidated, o.conislocal, o.coninhcount,
       o.connoinherit, o.conenforced
  FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE c.relname IN ('zze6gd_np','zze6gd_nc') ORDER BY 1, 2;

-- The clause is part of what the constraint is, so its definition says so too.
-- begin-expected
-- columns: def
-- row: NOT NULL j NOT VALID
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname = 'zze6gd_nn'
  AND conrelid = 'zze6gd_np'::regclass;

-- begin-expected
-- columns: relname | attname | attnotnull
-- row: zze6gd_nc | j | t
-- row: zze6gd_np | j | t
-- end-expected
SELECT c.relname, a.attname, a.attnotnull FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
 WHERE c.relname IN ('zze6gd_np','zze6gd_nc') AND a.attname = 'j' ORDER BY 1;

-- The rows that were already there stay.
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM zze6gd_np;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zze6gd_np" violates not-null constraint
-- end-expected-error
INSERT INTO zze6gd_np VALUES (3, NULL);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zze6gd_nc" violates not-null constraint
-- end-expected-error
INSERT INTO zze6gd_nc VALUES (4, NULL);

-- VALIDATE CONSTRAINT is what reads the rows, and it names the column and the
-- relation whose rows it read.
-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "j" of relation "zze6gd_np" contains null values
-- end-expected-error
ALTER TABLE zze6gd_np VALIDATE CONSTRAINT zze6gd_nn;

-- begin-expected
-- columns: relname | convalidated
-- row: zze6gd_nc | f
-- row: zze6gd_np | f
-- end-expected
SELECT c.relname, o.convalidated FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE o.conname = 'zze6gd_nn' ORDER BY 1;

UPDATE zze6gd_np SET j = 5;
ALTER TABLE zze6gd_np VALIDATE CONSTRAINT zze6gd_nn;

-- Validating on the relation that declared it settles the child's copy too.
-- begin-expected
-- columns: relname | convalidated
-- row: zze6gd_nc | t
-- row: zze6gd_np | t
-- end-expected
SELECT c.relname, o.convalidated FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE o.conname = 'zze6gd_nn' ORDER BY 1;

-- Validating what has already been validated has nothing to do, and the clause
-- is gone from the definition.
ALTER TABLE zze6gd_np VALIDATE CONSTRAINT zze6gd_nn;

-- begin-expected
-- columns: def
-- row: NOT NULL j
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname = 'zze6gd_nn'
  AND conrelid = 'zze6gd_np'::regclass;

DROP TABLE zze6gd_nc;
DROP TABLE zze6gd_np;

-- ============================================================================
-- A row that was already there may stay, but not be rewritten
-- ============================================================================
CREATE TABLE zze6gd_u (i int, j int);
INSERT INTO zze6gd_u VALUES (1, NULL), (2, 5);
ALTER TABLE zze6gd_u ADD CONSTRAINT zze6gd_un NOT NULL j NOT VALID;

-- begin-expected
-- columns: i | j
-- row: 1 | NULL
-- row: 2 | 5
-- end-expected
SELECT i, j FROM zze6gd_u ORDER BY i;

-- Writing the row again is writing a null into the column, so it is refused
-- even where the statement never named that column.
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zze6gd_u" violates not-null constraint
-- end-expected-error
UPDATE zze6gd_u SET i = 9 WHERE i = 1;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zze6gd_u" violates not-null constraint
-- end-expected-error
UPDATE zze6gd_u SET j = NULL WHERE i = 2;

UPDATE zze6gd_u SET j = 3 WHERE i = 1;
ALTER TABLE zze6gd_u VALIDATE CONSTRAINT zze6gd_un;

-- begin-expected
-- columns: conname | convalidated
-- row: zze6gd_un | t
-- end-expected
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'zze6gd_u'::regclass;

DROP TABLE zze6gd_u;

-- ============================================================================
-- Which relation the rows are really in is the one a failed validation names
-- ============================================================================
CREATE TABLE zze6gd_vp (i int, j int);
CREATE TABLE zze6gd_vc () INHERITS (zze6gd_vp);
INSERT INTO zze6gd_vc VALUES (2, NULL);

ALTER TABLE zze6gd_vp ADD NOT NULL j NOT VALID;

-- begin-expected
-- columns: relname | conname | convalidated
-- row: zze6gd_vc | zze6gd_vp_j_not_null | f
-- row: zze6gd_vp | zze6gd_vp_j_not_null | f
-- end-expected
SELECT c.relname, o.conname, o.convalidated FROM pg_constraint o
  JOIN pg_class c ON c.oid = o.conrelid
 WHERE c.relname IN ('zze6gd_vp','zze6gd_vc') ORDER BY 1, 2;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "j" of relation "zze6gd_vc" contains null values
-- end-expected-error
ALTER TABLE zze6gd_vp VALIDATE CONSTRAINT zze6gd_vp_j_not_null;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "j" of relation "zze6gd_vc" contains null values
-- end-expected-error
ALTER TABLE zze6gd_vc VALIDATE CONSTRAINT zze6gd_vp_j_not_null;

UPDATE zze6gd_vc SET j = 7;

-- Validating on the child settles the child's copy alone: the relation that
-- declared the rule is still waiting for its own rows to be read.
ALTER TABLE zze6gd_vc VALIDATE CONSTRAINT zze6gd_vp_j_not_null;

-- begin-expected
-- columns: relname | convalidated
-- row: zze6gd_vc | t
-- row: zze6gd_vp | f
-- end-expected
SELECT c.relname, o.convalidated FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE c.relname IN ('zze6gd_vp','zze6gd_vc') ORDER BY 1;

ALTER TABLE zze6gd_vp VALIDATE CONSTRAINT zze6gd_vp_j_not_null;

-- begin-expected
-- columns: relname | convalidated
-- row: zze6gd_vc | t
-- row: zze6gd_vp | t
-- end-expected
SELECT c.relname, o.convalidated FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE c.relname IN ('zze6gd_vp','zze6gd_vc') ORDER BY 1;

DROP TABLE zze6gd_vc;
DROP TABLE zze6gd_vp;

-- ============================================================================
-- A partitioned table keeps its rows below, and that is where the scan looks
-- ============================================================================
CREATE TABLE zze6gd_pp (i int, j int) PARTITION BY RANGE (i);
CREATE TABLE zze6gd_p1 PARTITION OF zze6gd_pp FOR VALUES FROM (1) TO (10);
INSERT INTO zze6gd_pp VALUES (2, NULL);

ALTER TABLE zze6gd_pp ADD CONSTRAINT zze6gd_pn NOT NULL j NOT VALID;

-- begin-expected
-- columns: relname | conname | convalidated | conislocal | coninhcount
-- row: zze6gd_p1 | zze6gd_pn | f | f | 1
-- row: zze6gd_pp | zze6gd_pn | f | t | 0
-- end-expected
SELECT c.relname, o.conname, o.convalidated, o.conislocal, o.coninhcount
  FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE o.conname = 'zze6gd_pn' ORDER BY 1;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "j" of relation "zze6gd_p1" contains null values
-- end-expected-error
ALTER TABLE zze6gd_pp VALIDATE CONSTRAINT zze6gd_pn;

UPDATE zze6gd_pp SET j = 1;
ALTER TABLE zze6gd_pp VALIDATE CONSTRAINT zze6gd_pn;

-- begin-expected
-- columns: relname | convalidated
-- row: zze6gd_p1 | t
-- row: zze6gd_pp | t
-- end-expected
SELECT c.relname, o.convalidated FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE o.conname = 'zze6gd_pn' ORDER BY 1;

DROP TABLE zze6gd_pp;

-- A relation that joins the hierarchy afterwards brings no rows with it, so its
-- copy of the rule holds over everything it stores from the moment it is there.
CREATE TABLE zze6gd_zp (i int, j int) PARTITION BY RANGE (i);
ALTER TABLE zze6gd_zp ADD CONSTRAINT zze6gd_zn NOT NULL j NOT VALID;
CREATE TABLE zze6gd_z1 PARTITION OF zze6gd_zp FOR VALUES FROM (1) TO (10);

-- begin-expected
-- columns: relname | conname | convalidated
-- row: zze6gd_z1 | zze6gd_zn | t
-- row: zze6gd_zp | zze6gd_zn | f
-- end-expected
SELECT c.relname, o.conname, o.convalidated FROM pg_constraint o
  JOIN pg_class c ON c.oid = o.conrelid WHERE o.conname = 'zze6gd_zn' ORDER BY 1;

DROP TABLE zze6gd_zp;

CREATE TABLE zze6gd_yp (i int, j int);
ALTER TABLE zze6gd_yp ADD CONSTRAINT zze6gd_yn NOT NULL j NOT VALID;
CREATE TABLE zze6gd_yc () INHERITS (zze6gd_yp);

-- begin-expected
-- columns: relname | conname | convalidated
-- row: zze6gd_yc | zze6gd_yn | t
-- row: zze6gd_yp | zze6gd_yn | f
-- end-expected
SELECT c.relname, o.conname, o.convalidated FROM pg_constraint o
  JOIN pg_class c ON c.oid = o.conrelid WHERE o.conname = 'zze6gd_yn' ORDER BY 1;

DROP TABLE zze6gd_yc;
DROP TABLE zze6gd_yp;

-- ============================================================================
-- What a NOT NULL nobody has read yet will not stand under
-- ============================================================================
CREATE TABLE zze6gd_b (i int, j int);
ALTER TABLE zze6gd_b ADD CONSTRAINT zze6gd_bn NOT NULL j NOT VALID;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: incompatible NOT VALID constraint "zze6gd_bn" on relation "zze6gd_b"
-- hint-like: You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.
-- end-expected-error
ALTER TABLE zze6gd_b ADD CONSTRAINT zze6gd_bn2 NOT NULL j;

-- A second NOT VALID declaration is refused for what it would create: nothing.
-- The constraint already there keeps the column, and its own name.
-- note: PostgreSQL 18.0 merged this into the constraint already there; a later PostgreSQL 18 refuses it, and that refusal is what is asserted
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot create not-null constraint "zze6gd_bn3" on column "j" of table "zze6gd_b"
-- end-expected-error
ALTER TABLE zze6gd_b ADD CONSTRAINT zze6gd_bn3 NOT NULL j NOT VALID;

-- begin-expected
-- columns: conname | convalidated
-- row: zze6gd_bn | f
-- end-expected
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'zze6gd_b'::regclass;

DROP TABLE zze6gd_b;

CREATE TABLE zze6gd_kk (i int, j int);
INSERT INTO zze6gd_kk VALUES (1, NULL);
ALTER TABLE zze6gd_kk ADD CONSTRAINT zze6gd_kn NOT NULL j NOT VALID;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot create primary key on column "j"
-- detail-like: The constraint "zze6gd_kn" on column "j" of table "zze6gd_kk", marked NOT VALID, is incompatible with a primary key.
-- hint-like: You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.
-- end-expected-error
ALTER TABLE zze6gd_kk ADD PRIMARY KEY (j);

-- begin-expected
-- columns: conname | contype | convalidated
-- row: zze6gd_kn | n | f
-- end-expected
SELECT conname, contype, convalidated FROM pg_constraint
  WHERE conrelid = 'zze6gd_kk'::regclass ORDER BY 1;

-- SET NOT NULL reads the rows, so it settles what the declaration left open.
-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "j" of relation "zze6gd_kk" contains null values
-- end-expected-error
ALTER TABLE zze6gd_kk ALTER COLUMN j SET NOT NULL;

-- begin-expected
-- columns: conname | convalidated
-- row: zze6gd_kn | f
-- end-expected
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'zze6gd_kk'::regclass;

UPDATE zze6gd_kk SET j = 4;
ALTER TABLE zze6gd_kk ALTER COLUMN j SET NOT NULL;

-- begin-expected
-- columns: conname | convalidated
-- row: zze6gd_kn | t
-- end-expected
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'zze6gd_kk'::regclass;

DROP TABLE zze6gd_kk;

-- A rule the column already carries and has been held to is not reopened by
-- NOT VALID: the declaration creates nothing at all, and is refused for it.
CREATE TABLE zze6gd_hh (i int, j int NOT NULL);

-- note: PostgreSQL 18.0 merged this into the constraint already there; a later PostgreSQL 18 refuses it, and that refusal is what is asserted
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot create not-null constraint "zze6gd_hn" on column "j" of table "zze6gd_hh"
-- end-expected-error
ALTER TABLE zze6gd_hh ADD CONSTRAINT zze6gd_hn NOT NULL j NOT VALID;

-- begin-expected
-- columns: conname | convalidated
-- row: zze6gd_hh_j_not_null | t
-- end-expected
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'zze6gd_hh'::regclass;

DROP TABLE zze6gd_hh;

-- Dropping the constraint takes the rule with it, and so does DROP NOT NULL
-- written on the column.
CREATE TABLE zze6gd_dd (i int, j int);
INSERT INTO zze6gd_dd VALUES (1, NULL);
ALTER TABLE zze6gd_dd ADD CONSTRAINT zze6gd_dn NOT NULL j NOT VALID;
ALTER TABLE zze6gd_dd DROP CONSTRAINT zze6gd_dn;

-- begin-expected
-- columns: attname | attnotnull
-- row: i | f
-- row: j | f
-- end-expected
SELECT attname, attnotnull FROM pg_attribute
  WHERE attrelid = 'zze6gd_dd'::regclass AND attnum > 0 ORDER BY attnum;

ALTER TABLE zze6gd_dd ADD CONSTRAINT zze6gd_dn NOT NULL j NOT VALID;
ALTER TABLE zze6gd_dd ALTER COLUMN j DROP NOT NULL;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint WHERE conrelid = 'zze6gd_dd'::regclass;

INSERT INTO zze6gd_dd VALUES (2, NULL);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM zze6gd_dd;

DROP TABLE zze6gd_dd;

-- ============================================================================
-- A validation is undone with the transaction that made it
-- ============================================================================
CREATE TABLE zze6gd_rr (i int, j int);
INSERT INTO zze6gd_rr VALUES (1, NULL);
ALTER TABLE zze6gd_rr ADD CONSTRAINT zze6gd_rn NOT NULL j NOT VALID;
UPDATE zze6gd_rr SET j = 2;

BEGIN;
ALTER TABLE zze6gd_rr VALIDATE CONSTRAINT zze6gd_rn;

-- begin-expected
-- columns: convalidated
-- row: t
-- end-expected
SELECT convalidated FROM pg_constraint WHERE conname = 'zze6gd_rn';

ROLLBACK;

-- begin-expected
-- columns: convalidated
-- row: f
-- end-expected
SELECT convalidated FROM pg_constraint WHERE conname = 'zze6gd_rn';

-- And so is the declaration itself.
BEGIN;
ALTER TABLE zze6gd_rr ADD CONSTRAINT zze6gd_rn2 NOT NULL i NOT VALID;
ROLLBACK;

-- begin-expected
-- columns: attname | attnotnull
-- row: i | f
-- row: j | t
-- end-expected
SELECT attname, attnotnull FROM pg_attribute
  WHERE attrelid = 'zze6gd_rr'::regclass AND attnum > 0 ORDER BY attnum;

DROP TABLE zze6gd_rr;

-- ============================================================================
-- A CHECK declared NOT VALID is validated over the rows the relation stands for
-- ============================================================================
CREATE TABLE zze6gd_cp (i int, j int);
CREATE TABLE zze6gd_cc () INHERITS (zze6gd_cp);
INSERT INTO zze6gd_cc VALUES (1, 0);
ALTER TABLE zze6gd_cp ADD CONSTRAINT zze6gd_ck CHECK (j > 0) NOT VALID;

-- begin-expected
-- columns: def
-- row: CHECK ((j > 0)) NOT VALID
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname = 'zze6gd_ck'
  AND conrelid = 'zze6gd_cp'::regclass;

-- The row already there may stay; a row written now is held to the rule.
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zze6gd_cc" violates check constraint "zze6gd_ck"
-- end-expected-error
INSERT INTO zze6gd_cc VALUES (2, -1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: check constraint "zze6gd_ck" of relation "zze6gd_cc" is violated by some row
-- end-expected-error
ALTER TABLE zze6gd_cp VALIDATE CONSTRAINT zze6gd_ck;

UPDATE zze6gd_cc SET j = 5;
ALTER TABLE zze6gd_cp VALIDATE CONSTRAINT zze6gd_ck;

-- begin-expected
-- columns: relname | convalidated
-- row: zze6gd_cc | t
-- row: zze6gd_cp | t
-- end-expected
SELECT c.relname, o.convalidated FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE o.conname = 'zze6gd_ck' ORDER BY 1;

-- begin-expected
-- columns: def
-- row: CHECK ((j > 0))
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname = 'zze6gd_ck'
  AND conrelid = 'zze6gd_cp'::regclass;

DROP TABLE zze6gd_cc;
DROP TABLE zze6gd_cp;

-- Validating on a child settles that child alone, a CHECK as a NOT NULL.
CREATE TABLE zze6gd_c3 (i int, j int);
CREATE TABLE zze6gd_c4 () INHERITS (zze6gd_c3);
INSERT INTO zze6gd_c4 VALUES (1, 5);
ALTER TABLE zze6gd_c3 ADD CONSTRAINT zze6gd_ck2 CHECK (j > 0) NOT VALID;
ALTER TABLE zze6gd_c4 VALIDATE CONSTRAINT zze6gd_ck2;

-- begin-expected
-- columns: relname | convalidated
-- row: zze6gd_c3 | f
-- row: zze6gd_c4 | t
-- end-expected
SELECT c.relname, o.convalidated FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid
 WHERE o.conname = 'zze6gd_ck2' ORDER BY 1;

DROP TABLE zze6gd_c4;
DROP TABLE zze6gd_c3;
