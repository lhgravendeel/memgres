-- ============================================================================
-- A partition's copy of an index is named for the partition, whichever
-- statement came second
-- ============================================================================

CREATE TABLE zzw5b_uy (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzw5b_uy_0 PARTITION OF zzw5b_uy FOR VALUES FROM (0) TO (10);
CREATE UNIQUE INDEX zzw5b_uy_idx ON zzw5b_uy (i);

-- stmt: an index made after the partition still names the copy after the partition
-- begin-expected
-- columns: relname | indisunique
-- row: zzw5b_uy_0_i_idx | t
-- end-expected
SELECT ic.relname, i.indisunique FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid
 JOIN pg_class ic ON i.indexrelid = ic.oid
 WHERE c.relname = 'zzw5b_uy_0' ORDER BY 1;

INSERT INTO zzw5b_uy VALUES (3, 'a');

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zzw5b_uy_0_i_idx"
-- end-expected-error
INSERT INTO zzw5b_uy VALUES (3, 'b');

DROP TABLE zzw5b_uy;

CREATE TABLE zzw5b_qy (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzw5b_qy_0 PARTITION OF zzw5b_qy FOR VALUES FROM (0) TO (10);
CREATE INDEX zzw5b_qy_idx ON zzw5b_qy (s);

-- stmt: a non-unique index is copied under the same derived name
-- begin-expected
-- columns: indexname
-- row: zzw5b_qy_0_s_idx
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzw5b_qy_0' ORDER BY 1;

DROP TABLE zzw5b_qy;

-- ============================================================================
-- A table attached as a partition is indexed the way one created inside the
-- hierarchy is
-- ============================================================================

CREATE TABLE zzw5b_at (i int, s text) PARTITION BY RANGE (i);
CREATE INDEX zzw5b_at_idx ON zzw5b_at (s);
CREATE TABLE zzw5b_at_0 (i int, s text);
ALTER TABLE zzw5b_at ATTACH PARTITION zzw5b_at_0 FOR VALUES FROM (0) TO (10);

-- stmt: ATTACH PARTITION gives the incoming table a copy of the parent's index
-- begin-expected
-- columns: relname | indisunique
-- row: zzw5b_at_0_s_idx | f
-- end-expected
SELECT ic.relname, i.indisunique FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid
 JOIN pg_class ic ON i.indexrelid = ic.oid
 WHERE c.relname = 'zzw5b_at_0' ORDER BY 1;

DROP TABLE zzw5b_at;

CREATE TABLE zzw5b_a2 (i int, s text) PARTITION BY RANGE (i);
CREATE INDEX zzw5b_a2_idx ON zzw5b_a2 (s);
CREATE TABLE zzw5b_a2_0 (i int, s text);
CREATE INDEX zzw5b_a2_own ON zzw5b_a2_0 (s);
ALTER TABLE zzw5b_a2 ATTACH PARTITION zzw5b_a2_0 FOR VALUES FROM (0) TO (10);

-- stmt: an index the attached table already has is the copy, not a second one over the same rows
-- begin-expected
-- columns: relname
-- row: zzw5b_a2_own
-- end-expected
SELECT ic.relname FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid
 JOIN pg_class ic ON i.indexrelid = ic.oid
 WHERE c.relname = 'zzw5b_a2_0' ORDER BY 1;

DROP TABLE zzw5b_a2;

CREATE TABLE zzw5b_m (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzw5b_m_0 PARTITION OF zzw5b_m FOR VALUES FROM (0) TO (10);
CREATE INDEX zzw5b_m_own ON zzw5b_m_0 (s);
CREATE INDEX zzw5b_m_idx ON zzw5b_m (s);

-- stmt: CREATE INDEX on the partitioned table adopts a matching index a partition already has
-- begin-expected
-- columns: relname
-- row: zzw5b_m_own
-- end-expected
SELECT ic.relname FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid
 JOIN pg_class ic ON i.indexrelid = ic.oid
 WHERE c.relname = 'zzw5b_m_0' ORDER BY 1;

DROP TABLE zzw5b_m;

CREATE TABLE zzw5b_n (i int, s text) PARTITION BY RANGE (i);
CREATE UNIQUE INDEX zzw5b_n_idx ON zzw5b_n (i);
CREATE TABLE zzw5b_n_0 (i int, s text);
CREATE INDEX zzw5b_n_own ON zzw5b_n_0 (i);
ALTER TABLE zzw5b_n ATTACH PARTITION zzw5b_n_0 FOR VALUES FROM (0) TO (10);

-- stmt: a non-unique index does not answer for a unique one, so the copy is still made
-- begin-expected
-- columns: relname | indisunique
-- row: zzw5b_n_0_i_idx | t
-- row: zzw5b_n_own | f
-- end-expected
SELECT ic.relname, i.indisunique FROM pg_index i
 JOIN pg_class c ON i.indrelid = c.oid
 JOIN pg_class ic ON i.indexrelid = ic.oid
 WHERE c.relname = 'zzw5b_n_0' ORDER BY 1;

INSERT INTO zzw5b_n VALUES (3, 'a');

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zzw5b_n_0_i_idx"
-- end-expected-error
INSERT INTO zzw5b_n VALUES (3, 'b');

DROP TABLE zzw5b_n;

-- ============================================================================
-- A constraint a relation holds because a parent has it is not its own
-- ============================================================================

CREATE TABLE zzw5b_cl (a int CHECK (a > 0), b int NOT NULL, c int PRIMARY KEY);
CREATE TABLE zzw5b_clc () INHERITS (zzw5b_cl);

-- stmt: what the parent declares is local to the parent
-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzw5b_cl_a_check | c | t | 0
-- row: zzw5b_cl_b_not_null | n | t | 0
-- row: zzw5b_cl_c_not_null | n | t | 0
-- row: zzw5b_cl_pkey | p | t | 0
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_cl' ORDER BY 1;

-- stmt: the inheritance child keeps the parent's names and is told it inherited them
-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzw5b_cl_a_check | c | f | 1
-- row: zzw5b_cl_b_not_null | n | f | 1
-- row: zzw5b_cl_c_not_null | n | f | 1
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_clc' ORDER BY 1;

DROP TABLE zzw5b_clc;
DROP TABLE zzw5b_cl;

CREATE TABLE zzw5b_pl (a int NOT NULL, b text, c int, PRIMARY KEY (a)) PARTITION BY RANGE (a);
CREATE TABLE zzw5b_pl_0 PARTITION OF zzw5b_pl FOR VALUES FROM (0) TO (10);
ALTER TABLE zzw5b_pl ADD CONSTRAINT zzw5b_plk CHECK (b <> 'bad');

-- stmt: a partition obeys the partitioned table's key, NOT NULL and CHECK without declaring them
-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzw5b_pl_0_pkey | p | f | 1
-- row: zzw5b_pl_a_not_null | n | f | 1
-- row: zzw5b_plk | c | f | 1
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_pl_0' ORDER BY 1;

DROP TABLE zzw5b_pl;

CREATE TABLE zzw5b_ref (id int PRIMARY KEY);
CREATE TABLE zzw5b_fk (a int, r int REFERENCES zzw5b_ref(id), UNIQUE (a, r)) PARTITION BY RANGE (a);
CREATE TABLE zzw5b_fk_0 PARTITION OF zzw5b_fk FOR VALUES FROM (0) TO (10);

-- stmt: the partition's unique key is renamed for itself, the foreign key keeps its name, both inherited
-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzw5b_fk_0_a_r_key | u | f | 1
-- row: zzw5b_fk_r_fkey | f | f | 1
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_fk_0' ORDER BY 1;

DROP TABLE zzw5b_fk;
DROP TABLE zzw5b_ref;