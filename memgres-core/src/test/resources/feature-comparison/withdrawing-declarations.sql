-- ============================================================================
-- A rule handed down can be withdrawn: DROP CONSTRAINT reaches the copies
-- ============================================================================

CREATE TABLE zzw4b_ph (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw4b_ph_0 PARTITION OF zzw4b_ph FOR VALUES FROM (0) TO (10);
ALTER TABLE zzw4b_ph ADD CONSTRAINT zzw4b_phk CHECK (b <> 'bad');
ALTER TABLE zzw4b_ph DROP CONSTRAINT zzw4b_phk;

-- stmt: the partition's copy went with the parent's
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint
 WHERE conrelid = 'zzw4b_ph_0'::regclass AND contype = 'c';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "zzw4b_phk" of relation "zzw4b_ph_0" does not exist
-- end-expected-error
ALTER TABLE zzw4b_ph_0 DROP CONSTRAINT zzw4b_phk;

INSERT INTO zzw4b_ph_0 VALUES (1, 'bad');

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_ph;

DROP TABLE zzw4b_ph;

CREATE TABLE zzw4b_ih (a int, b text);
CREATE TABLE zzw4b_ihc () INHERITS (zzw4b_ih);
ALTER TABLE zzw4b_ih ADD CONSTRAINT zzw4b_ihk CHECK (b <> 'bad');
ALTER TABLE zzw4b_ih DROP CONSTRAINT zzw4b_ihk;

-- stmt: an inheritance child's copy is withdrawn the same way
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint
 WHERE conrelid = 'zzw4b_ihc'::regclass AND contype = 'c';

INSERT INTO zzw4b_ihc VALUES (1, 'bad');

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_ihc;

DROP TABLE zzw4b_ihc;
DROP TABLE zzw4b_ih;

CREATE TABLE zzw4b_rf (id int PRIMARY KEY);
CREATE TABLE zzw4b_fk (a int, r int) PARTITION BY RANGE (a);
CREATE TABLE zzw4b_fk_0 PARTITION OF zzw4b_fk FOR VALUES FROM (0) TO (10);
ALTER TABLE zzw4b_fk ADD CONSTRAINT zzw4b_fkc FOREIGN KEY (r) REFERENCES zzw4b_rf(id);
ALTER TABLE zzw4b_fk DROP CONSTRAINT zzw4b_fkc;

-- stmt: and so is a foreign key the partitioned table declared
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint
 WHERE conrelid = 'zzw4b_fk_0'::regclass AND contype = 'f';

INSERT INTO zzw4b_fk VALUES (1, 99);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_fk;

DROP TABLE zzw4b_fk;
DROP TABLE zzw4b_rf;

-- ============================================================================
-- Leaving a hierarchy makes the constraints a table keeps its own
-- ============================================================================

CREATE TABLE zzw4b_j6 (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw4b_j6_0 PARTITION OF zzw4b_j6 FOR VALUES FROM (0) TO (10);
ALTER TABLE zzw4b_j6 ADD CONSTRAINT zzw4b_j6k CHECK (b <> 'bad');
ALTER TABLE zzw4b_j6 DETACH PARTITION zzw4b_j6_0;

-- stmt: the detached table keeps the constraint
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint
 WHERE conrelid = 'zzw4b_j6_0'::regclass AND contype = 'c';

-- stmt: and it is the detached table's own, so the detached table may drop it
ALTER TABLE zzw4b_j6_0 DROP CONSTRAINT zzw4b_j6k;

INSERT INTO zzw4b_j6_0 VALUES (1, 'bad');

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_j6_0;

DROP TABLE zzw4b_j6_0;
DROP TABLE zzw4b_j6;

CREATE TABLE zzw4b_np (a int, b text);
ALTER TABLE zzw4b_np ADD CONSTRAINT zzw4b_nk CHECK (b <> 'bad');
CREATE TABLE zzw4b_nc () INHERITS (zzw4b_np);
ALTER TABLE zzw4b_nc NO INHERIT zzw4b_np;

-- stmt: NO INHERIT hands the CHECK over the same way DETACH does
ALTER TABLE zzw4b_nc DROP CONSTRAINT zzw4b_nk;

INSERT INTO zzw4b_nc VALUES (1, 'bad');

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_nc;

DROP TABLE zzw4b_nc;
DROP TABLE zzw4b_np;

CREATE TABLE zzw4b_o1 (a int, b text);
CREATE TABLE zzw4b_o1c () INHERITS (zzw4b_o1);
ALTER TABLE zzw4b_o1 ADD CONSTRAINT zzw4b_o1k CHECK (b <> 'bad');

-- stmt: ONLY drops the parent's own and leaves the child holding its copy
ALTER TABLE ONLY zzw4b_o1 DROP CONSTRAINT zzw4b_o1k;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzw4b_o1c" violates check constraint "zzw4b_o1k"
-- end-expected-error
INSERT INTO zzw4b_o1c VALUES (1, 'bad');

-- stmt: which the child may then drop, because the copy is now its own
ALTER TABLE zzw4b_o1c DROP CONSTRAINT zzw4b_o1k;

INSERT INTO zzw4b_o1c VALUES (1, 'bad');

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_o1c;

DROP TABLE zzw4b_o1c;
DROP TABLE zzw4b_o1;

-- ============================================================================
-- A NOT NULL belongs to the relation that declared it
-- ============================================================================

CREATE TABLE zzw4b_j1 (a int, b text);
CREATE TABLE zzw4b_j1c () INHERITS (zzw4b_j1);
ALTER TABLE zzw4b_j1 ALTER COLUMN b SET NOT NULL;
ALTER TABLE zzw4b_j1 ALTER COLUMN b DROP NOT NULL;

-- stmt: dropping it on the parent drops it on the inheritance child too
-- begin-expected
-- columns: is_nullable
-- row: YES
-- end-expected
SELECT is_nullable FROM information_schema.columns
 WHERE table_name = 'zzw4b_j1c' AND column_name = 'b';

INSERT INTO zzw4b_j1c (a) VALUES (1);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_j1c;

DROP TABLE zzw4b_j1c;
DROP TABLE zzw4b_j1;

CREATE TABLE zzw4b_on (a int, b text NOT NULL);
CREATE TABLE zzw4b_onc () INHERITS (zzw4b_on);

-- stmt: ONLY asks for the named relation alone, and the child keeps the rule
ALTER TABLE ONLY zzw4b_on ALTER COLUMN b DROP NOT NULL;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "b" of relation "zzw4b_onc" violates not-null constraint
-- end-expected-error
INSERT INTO zzw4b_onc (a) VALUES (1);

DROP TABLE zzw4b_onc;
DROP TABLE zzw4b_on;

CREATE TABLE zzw4b_j3 (a int, b text NOT NULL) PARTITION BY RANGE (a);
CREATE TABLE zzw4b_j3_0 PARTITION OF zzw4b_j3 FOR VALUES FROM (0) TO (10);

-- stmt: a partition may not take off what the partitioned table declares
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "b" is marked NOT NULL in parent table
-- end-expected-error
ALTER TABLE zzw4b_j3_0 ALTER COLUMN b DROP NOT NULL;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "b" of relation "zzw4b_j3_0" violates not-null constraint
-- end-expected-error
INSERT INTO zzw4b_j3_0 (a) VALUES (1);

DROP TABLE zzw4b_j3;

CREATE TABLE zzw4b_j4 (a int, b text NOT NULL);
CREATE TABLE zzw4b_j4c () INHERITS (zzw4b_j4);

-- stmt: an inheritance child is told which constraint it took from where
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzw4b_j4_b_not_null" of relation "zzw4b_j4c"
-- end-expected-error
ALTER TABLE zzw4b_j4c ALTER COLUMN b DROP NOT NULL;

DROP TABLE zzw4b_j4c;
DROP TABLE zzw4b_j4;

CREATE TABLE zzw4b_q1 (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw4b_q1_0 PARTITION OF zzw4b_q1 FOR VALUES FROM (0) TO (10);

-- stmt: a NOT NULL the partition declared for itself is the partition's to drop
ALTER TABLE zzw4b_q1_0 ALTER COLUMN b SET NOT NULL;
ALTER TABLE zzw4b_q1_0 ALTER COLUMN b DROP NOT NULL;

INSERT INTO zzw4b_q1_0 (a) VALUES (1);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM zzw4b_q1;

DROP TABLE zzw4b_q1;

-- ============================================================================
-- A partition's copy of an index on the partitioned table is named for itself
-- ============================================================================

CREATE TABLE zzw4b_ux (i int, s text) PARTITION BY RANGE (i);
CREATE UNIQUE INDEX zzw4b_ux_idx ON zzw4b_ux (i);
CREATE TABLE zzw4b_ux_0 PARTITION OF zzw4b_ux FOR VALUES FROM (0) TO (10);

-- stmt: one index, named after the partition and the column, not after the parent's index
-- begin-expected
-- columns: relname | indisunique
-- row: zzw4b_ux_0_i_idx | t
-- end-expected
SELECT ic.relname, i.indisunique FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid
 JOIN pg_class ic ON i.indexrelid = ic.oid
 WHERE c.relname = 'zzw4b_ux_0' ORDER BY 1;

INSERT INTO zzw4b_ux VALUES (5, 'a');

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zzw4b_ux_0_i_idx"
-- end-expected-error
INSERT INTO zzw4b_ux VALUES (5, 'b');

DROP TABLE zzw4b_ux;

CREATE TABLE zzw4b_cx (i int, s text) PARTITION BY RANGE (i);
CREATE INDEX zzw4b_cx_a ON zzw4b_cx (i);
CREATE INDEX zzw4b_cx_b ON zzw4b_cx (i);
CREATE INDEX zzw4b_cx_c ON zzw4b_cx (s, i);
CREATE TABLE zzw4b_cx_0 PARTITION OF zzw4b_cx FOR VALUES FROM (0) TO (10);

-- stmt: two indexes over one column give the second copy the number PostgreSQL appends
-- begin-expected
-- columns: relname
-- row: zzw4b_cx_0_i_idx
-- row: zzw4b_cx_0_i_idx1
-- row: zzw4b_cx_0_s_i_idx
-- end-expected
SELECT ic.relname FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid
 JOIN pg_class ic ON i.indexrelid = ic.oid
 WHERE c.relname = 'zzw4b_cx_0' ORDER BY 1;

DROP TABLE zzw4b_cx;