-- ============================================================================
-- A constraint declared on a parent is the hierarchy's constraint
-- ============================================================================

CREATE TABLE zzw3b_p (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw3b_p_p0 PARTITION OF zzw3b_p FOR VALUES FROM (0) TO (10);
ALTER TABLE zzw3b_p ADD CONSTRAINT zzw3b_ck CHECK (b <> 'bad');

-- stmt: the partition holds the rows, so it has to carry the rule
-- begin-expected
-- columns: conname
-- row: zzw3b_ck
-- end-expected
SELECT conname FROM pg_constraint
 WHERE conrelid = 'zzw3b_p_p0'::regclass AND contype = 'c';

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzw3b_p_p0" violates check constraint "zzw3b_ck"
-- end-expected-error
INSERT INTO zzw3b_p_p0 VALUES (1, 'bad');

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_p;

-- stmt: and the rule is the parent's, so the partition may not drop it
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzw3b_ck" of relation "zzw3b_p_p0"
-- end-expected-error
ALTER TABLE zzw3b_p_p0 DROP CONSTRAINT zzw3b_ck;

DROP TABLE zzw3b_p;

CREATE TABLE zzw3b_par (a int, b text);
CREATE TABLE zzw3b_chi () INHERITS (zzw3b_par);
ALTER TABLE zzw3b_par ADD CONSTRAINT zzw3b_ck2 CHECK (a > 0);

-- begin-expected
-- columns: conname
-- row: zzw3b_ck2
-- end-expected
SELECT conname FROM pg_constraint
 WHERE conrelid = 'zzw3b_chi'::regclass AND contype = 'c';

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzw3b_chi" violates check constraint "zzw3b_ck2"
-- end-expected-error
INSERT INTO zzw3b_chi VALUES (-1, 'x');

-- stmt: a key is the child's own business and travels no further than its table
ALTER TABLE zzw3b_par ADD PRIMARY KEY (a);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint
 WHERE conrelid = 'zzw3b_chi'::regclass AND contype = 'p';

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzw3b_ck2" of relation "zzw3b_chi"
-- end-expected-error
ALTER TABLE zzw3b_chi DROP CONSTRAINT zzw3b_ck2;

DROP TABLE zzw3b_chi;
DROP TABLE zzw3b_par;

-- ============================================================================
-- CREATE TABLE ... INHERITS copies the parent's CHECK constraints
-- ============================================================================

CREATE TABLE zzw3b_cp2 (a int CHECK (a > 0));
CREATE TABLE zzw3b_cc2 () INHERITS (zzw3b_cp2);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint
 WHERE conrelid = 'zzw3b_cc2'::regclass AND contype = 'c';

-- stmt: the copy answers to the name the parent gave it
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzw3b_cc2" violates check constraint "zzw3b_cp2_a_check"
-- end-expected-error
INSERT INTO zzw3b_cc2 VALUES (-1);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_cc2;

DROP TABLE zzw3b_cc2;
DROP TABLE zzw3b_cp2;

-- stmt: NO INHERIT says the rule was never going to travel
CREATE TABLE zzw3b_ni (a int CHECK (a > 0) NO INHERIT);
CREATE TABLE zzw3b_nic () INHERITS (zzw3b_ni);
INSERT INTO zzw3b_nic VALUES (-1);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_nic;

DROP TABLE zzw3b_nic;
DROP TABLE zzw3b_ni;

-- stmt: and a table joining a parent has to carry the rule already
CREATE TABLE zzw3b_gdp (a int CHECK (a > 0));
CREATE TABLE zzw3b_gdc (a int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table is missing constraint "zzw3b_gdp_a_check"
-- end-expected-error
ALTER TABLE zzw3b_gdc INHERIT zzw3b_gdp;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_inherits WHERE inhrelid = 'zzw3b_gdc'::regclass;

-- stmt: the same rule under another name is not the rule
ALTER TABLE zzw3b_gdc ADD CONSTRAINT zzw3b_other CHECK (a > 0);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table is missing constraint "zzw3b_gdp_a_check"
-- end-expected-error
ALTER TABLE zzw3b_gdc INHERIT zzw3b_gdp;

CREATE TABLE zzw3b_gde (a int CONSTRAINT zzw3b_gdp_a_check CHECK (a > 0));
ALTER TABLE zzw3b_gde INHERIT zzw3b_gdp;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM pg_inherits WHERE inhrelid = 'zzw3b_gde'::regclass;

DROP TABLE zzw3b_gde;
DROP TABLE zzw3b_gdc;
DROP TABLE zzw3b_gdp;

-- ============================================================================
-- SET NOT NULL and SET DEFAULT reach the relations that hold the rows
-- ============================================================================

CREATE TABLE zzw3b_n1 (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw3b_n1_0 PARTITION OF zzw3b_n1 FOR VALUES FROM (0) TO (10);
ALTER TABLE zzw3b_n1 ALTER COLUMN b SET NOT NULL;

-- begin-expected
-- columns: is_nullable
-- row: NO
-- end-expected
SELECT is_nullable FROM information_schema.columns
 WHERE table_name = 'zzw3b_n1_0' AND column_name = 'b';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "b" of relation "zzw3b_n1_0" violates not-null constraint
-- end-expected-error
INSERT INTO zzw3b_n1_0 (a) VALUES (1);

-- stmt: and the partition stops refusing one when the partitioned table does
ALTER TABLE zzw3b_n1 ALTER COLUMN b DROP NOT NULL;

-- begin-expected
-- columns: is_nullable
-- row: YES
-- end-expected
SELECT is_nullable FROM information_schema.columns
 WHERE table_name = 'zzw3b_n1_0' AND column_name = 'b';

DROP TABLE zzw3b_n1;

CREATE TABLE zzw3b_n2 (a int, b text);
CREATE TABLE zzw3b_n2c () INHERITS (zzw3b_n2);
ALTER TABLE zzw3b_n2 ALTER COLUMN b SET NOT NULL;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "b" of relation "zzw3b_n2c" violates not-null constraint
-- end-expected-error
INSERT INTO zzw3b_n2c (a) VALUES (1);

DROP TABLE zzw3b_n2c;
DROP TABLE zzw3b_n2;

CREATE TABLE zzw3b_d2 (a int, b text);
CREATE TABLE zzw3b_d2c () INHERITS (zzw3b_d2);
ALTER TABLE zzw3b_d2 ALTER COLUMN b SET DEFAULT 'q';
INSERT INTO zzw3b_d2c (a) VALUES (1);

-- begin-expected
-- columns: a | b
-- row: 1 | q
-- end-expected
SELECT a, b FROM zzw3b_d2c ORDER BY a;

-- stmt: ONLY asks for the named relation alone
ALTER TABLE ONLY zzw3b_d2 ALTER COLUMN b SET DEFAULT 'only';

-- begin-expected
-- columns: column_default
-- row: 'q'::text
-- end-expected
SELECT column_default FROM information_schema.columns
 WHERE table_name = 'zzw3b_d2c' AND column_name = 'b';

ALTER TABLE zzw3b_d2 ALTER COLUMN b DROP DEFAULT;

-- begin-expected
-- columns: column_default
-- row: null
-- end-expected
SELECT column_default FROM information_schema.columns
 WHERE table_name = 'zzw3b_d2c' AND column_name = 'b';

DROP TABLE zzw3b_d2c;
DROP TABLE zzw3b_d2;

-- stmt: a default on the parent replaces the one a partition set for itself
CREATE TABLE zzw3b_gf (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzw3b_gf_1 PARTITION OF zzw3b_gf FOR VALUES FROM (1) TO (10);
ALTER TABLE zzw3b_gf_1 ALTER COLUMN s SET DEFAULT 'child';
ALTER TABLE zzw3b_gf ALTER COLUMN s SET DEFAULT 'parent';

-- begin-expected
-- columns: column_default
-- row: 'parent'::text
-- end-expected
SELECT column_default FROM information_schema.columns
 WHERE table_name = 'zzw3b_gf_1' AND column_name = 's';

DROP TABLE zzw3b_gf;

-- ============================================================================
-- A new rule is judged against the rows already stored below
-- ============================================================================

CREATE TABLE zzw3b_vc (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw3b_vc_1 PARTITION OF zzw3b_vc FOR VALUES FROM (0) TO (10);
INSERT INTO zzw3b_vc VALUES (1, 'bad');

-- stmt: the parent stores nothing of its own, so its own rows say nothing
-- begin-expected-error
-- sqlstate: 23514
-- message-like: check constraint "zzw3b_vck" of relation "zzw3b_vc_1" is violated by some row
-- end-expected-error
ALTER TABLE zzw3b_vc ADD CONSTRAINT zzw3b_vck CHECK (b <> 'bad');

DROP TABLE zzw3b_vc;

CREATE TABLE zzw3b_nn (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw3b_nn_1 PARTITION OF zzw3b_nn FOR VALUES FROM (0) TO (10);
INSERT INTO zzw3b_nn (a) VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "b" of relation "zzw3b_nn_1" contains null values
-- end-expected-error
ALTER TABLE zzw3b_nn ALTER COLUMN b SET NOT NULL;

DROP TABLE zzw3b_nn;

CREATE TABLE zzw3b_ip (a int, b text);
CREATE TABLE zzw3b_ic () INHERITS (zzw3b_ip);
INSERT INTO zzw3b_ic VALUES (1, NULL);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "b" of relation "zzw3b_ic" contains null values
-- end-expected-error
ALTER TABLE zzw3b_ip ALTER COLUMN b SET NOT NULL;

INSERT INTO zzw3b_ic VALUES (2, 'x');

-- begin-expected-error
-- sqlstate: 23514
-- message-like: check constraint "zzw3b_ick" of relation "zzw3b_ic" is violated by some row
-- end-expected-error
ALTER TABLE zzw3b_ip ADD CONSTRAINT zzw3b_ick CHECK (a > 5);

DROP TABLE zzw3b_ic;
DROP TABLE zzw3b_ip;

-- ============================================================================
-- TRUNCATE of an inheritance parent empties its children
-- ============================================================================

CREATE TABLE zzw3b_tp (i int);
CREATE TABLE zzw3b_tpc () INHERITS (zzw3b_tp);
INSERT INTO zzw3b_tp VALUES (1);
INSERT INTO zzw3b_tpc VALUES (2);
TRUNCATE ONLY zzw3b_tp;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_tpc;

TRUNCATE zzw3b_tp;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_tpc;

DROP TABLE zzw3b_tpc;
DROP TABLE zzw3b_tp;

-- ============================================================================
-- What a partition inherits, and what its partitioned table may not change
-- ============================================================================

CREATE TABLE zzw3b_fh (i int, s text) PARTITION BY RANGE (i);
CREATE INDEX zzw3b_fh_idx ON zzw3b_fh (s);
CREATE TABLE zzw3b_fh_1 PARTITION OF zzw3b_fh FOR VALUES FROM (1) TO (10);

-- stmt: an index on a partitioned table reaches a partition created afterwards
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'zzw3b_fh_1';

DROP TABLE zzw3b_fh;

CREATE TABLE zzw3b_dr (i int) PARTITION BY RANGE (i);
CREATE TABLE zzw3b_dr_d PARTITION OF zzw3b_dr DEFAULT;
INSERT INTO zzw3b_dr VALUES (5);

-- stmt: the new bound would claim a row the default is already holding
-- begin-expected-error
-- sqlstate: 23514
-- message-like: updated partition constraint for default partition "zzw3b_dr_d" would be violated by some row
-- end-expected-error
CREATE TABLE zzw3b_dr_1 PARTITION OF zzw3b_dr FOR VALUES FROM (1) TO (10);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_class WHERE relname = 'zzw3b_dr_1';

-- stmt: a bound that claims none of the default's rows is accepted
CREATE TABLE zzw3b_dr_2 PARTITION OF zzw3b_dr FOR VALUES FROM (20) TO (30);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_dr_d;

DROP TABLE zzw3b_dr;

CREATE TABLE zzw3b_kc (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzw3b_kc_1 PARTITION OF zzw3b_kc FOR VALUES FROM (1) TO (10);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop column "i" because it is part of the partition key of relation "zzw3b_kc"
-- end-expected-error
ALTER TABLE zzw3b_kc DROP COLUMN i;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter column "i" because it is part of the partition key of relation "zzw3b_kc"
-- end-expected-error
ALTER TABLE zzw3b_kc ALTER COLUMN i TYPE bigint;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::int AS cnt FROM information_schema.columns
 WHERE table_name = 'zzw3b_kc';

-- stmt: a column the key does not read is dropped as it always was
ALTER TABLE zzw3b_kc DROP COLUMN s;

DROP TABLE zzw3b_kc;

-- stmt: an expression key stands for every column it names, with no partitions yet
CREATE TABLE zzw3b_ke (i int, s text) PARTITION BY RANGE ((i + 1));

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop column "i" because it is part of the partition key of relation "zzw3b_ke"
-- end-expected-error
ALTER TABLE zzw3b_ke DROP COLUMN i;

ALTER TABLE zzw3b_ke DROP COLUMN s;
DROP TABLE zzw3b_ke;

CREATE TABLE zzw3b_at (i int NOT NULL, s text) PARTITION BY RANGE (i);
CREATE TABLE zzw3b_at_c (i int, s text);
INSERT INTO zzw3b_at_c VALUES (5, 'a');

-- stmt: a child may not take a null in a column the parent declares NOT NULL
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "i" in child table "zzw3b_at_c" must be marked NOT NULL
-- end-expected-error
ALTER TABLE zzw3b_at ATTACH PARTITION zzw3b_at_c FOR VALUES FROM (1) TO (10);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_at;

ALTER TABLE zzw3b_at_c ALTER COLUMN i SET NOT NULL;
ALTER TABLE zzw3b_at ATTACH PARTITION zzw3b_at_c FOR VALUES FROM (1) TO (10);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw3b_at;

DROP TABLE zzw3b_at;

-- stmt: the rule is about the parent's nullability, not about the key itself
CREATE TABLE zzw3b_a2 (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzw3b_a2_c (i int, s text);
ALTER TABLE zzw3b_a2 ATTACH PARTITION zzw3b_a2_c FOR VALUES FROM (1) TO (10);

-- begin-expected
-- columns: is_nullable
-- row: YES
-- end-expected
SELECT is_nullable FROM information_schema.columns
 WHERE table_name = 'zzw3b_a2_c' AND column_name = 'i';

DROP TABLE zzw3b_a2;