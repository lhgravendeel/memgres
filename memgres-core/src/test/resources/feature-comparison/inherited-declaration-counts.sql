-- ============================================================================
-- What a relation declared itself and how many parents hand it the same thing
-- are two separate answers
-- ============================================================================

CREATE TABLE zzt9x_p0 (i int CONSTRAINT zzt9x_ck CHECK (i > 0));
CREATE TABLE zzt9x_p1 (i int CONSTRAINT zzt9x_ck CHECK (i > 0));
CREATE TABLE zzt9x_c (i int CONSTRAINT zzt9x_ck CHECK (i > 0)) INHERITS (zzt9x_p0, zzt9x_p1);
CREATE TABLE zzt9x_g () INHERITS (zzt9x_c);

-- stmt: the child declared the rule itself AND takes it from both parents; the grandchild
-- only takes it, from the one parent it has
-- begin-expected
-- columns: relname | conname | conislocal | coninhcount
-- row: zzt9x_c | zzt9x_ck | t | 2
-- row: zzt9x_g | zzt9x_ck | f | 1
-- row: zzt9x_p0 | zzt9x_ck | t | 0
-- row: zzt9x_p1 | zzt9x_ck | t | 0
-- end-expected
SELECT cl.relname, c.conname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g') AND c.contype = 'c'
 ORDER BY 1;

-- stmt: and the same two answers for the column the rule is about
-- begin-expected
-- columns: relname | attname | attislocal | attinhcount
-- row: zzt9x_c | i | t | 2
-- row: zzt9x_g | i | f | 1
-- row: zzt9x_p0 | i | t | 0
-- row: zzt9x_p1 | i | t | 0
-- end-expected
SELECT cl.relname, a.attname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g') AND a.attnum > 0
 ORDER BY 1;

ALTER TABLE zzt9x_c NO INHERIT zzt9x_p0;

-- stmt: NO INHERIT lets go of the parent it names and of no other
-- begin-expected
-- columns: parent | child
-- row: zzt9x_c | zzt9x_g
-- row: zzt9x_p1 | zzt9x_c
-- end-expected
SELECT p.relname AS parent, cl.relname AS child FROM pg_inherits h
 JOIN pg_class p ON p.oid = h.inhparent JOIN pg_class cl ON cl.oid = h.inhrelid
 WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g') ORDER BY 1, 2;

-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_c | t | 1
-- row: zzt9x_g | f | 1
-- row: zzt9x_p0 | t | 0
-- row: zzt9x_p1 | t | 0
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g') AND c.contype = 'c'
 ORDER BY 1;

-- begin-expected
-- columns: relname | attislocal | attinhcount
-- row: zzt9x_c | t | 1
-- row: zzt9x_g | f | 1
-- row: zzt9x_p0 | t | 0
-- row: zzt9x_p1 | t | 0
-- end-expected
SELECT cl.relname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g') AND a.attnum > 0
 ORDER BY 1;

ALTER TABLE zzt9x_c NO INHERIT zzt9x_p1;

-- stmt: with no parent left the child keeps what it declared, and counts nobody
-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_c | t | 0
-- row: zzt9x_g | f | 1
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_c','zzt9x_g') AND c.contype = 'c' ORDER BY 1;

-- begin-expected
-- columns: relname | attislocal | attinhcount
-- row: zzt9x_c | t | 0
-- row: zzt9x_g | f | 1
-- end-expected
SELECT cl.relname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_c','zzt9x_g') AND a.attnum > 0 ORDER BY 1;

DROP TABLE zzt9x_g;
DROP TABLE zzt9x_c;
DROP TABLE zzt9x_p0;
DROP TABLE zzt9x_p1;

-- stmt: a child that only takes the rule from two parents is local to neither
CREATE TABLE zzt9x_u0 (i int CONSTRAINT zzt9x_uck CHECK (i > 0));
CREATE TABLE zzt9x_u1 (i int CONSTRAINT zzt9x_uck CHECK (i > 0));
CREATE TABLE zzt9x_uc () INHERITS (zzt9x_u0, zzt9x_u1);

-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_uc | f | 2
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_uc' AND c.contype = 'c';

ALTER TABLE zzt9x_uc NO INHERIT zzt9x_u0;

-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_uc | f | 1
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_uc' AND c.contype = 'c';

DROP TABLE zzt9x_uc;
DROP TABLE zzt9x_u0;
DROP TABLE zzt9x_u1;

-- ============================================================================
-- A child that restates NOT NULL holds a constraint of its own name
-- ============================================================================

CREATE TABLE zzt9x_n0 (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzt9x_n1 (i int NOT NULL, k int NOT NULL) INHERITS (zzt9x_n0);

-- stmt: i is restated (the child's name, local, one parent), j is only taken (the parent's
-- name, not local), k is the child's alone (local, no parent)
-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzt9x_n0_j_not_null | n | f | 1
-- row: zzt9x_n1_i_not_null | n | t | 1
-- row: zzt9x_n1_k_not_null | n | t | 0
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_n1' ORDER BY 1;

-- begin-expected
-- columns: attname | attnotnull | attislocal | attinhcount
-- row: i | t | t | 1
-- row: j | t | f | 1
-- row: k | t | t | 1
-- end-expected
SELECT a.attname, a.attnotnull, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_n1' AND a.attnum > 0
 ORDER BY a.attnum;

DROP TABLE zzt9x_n1;
DROP TABLE zzt9x_n0;

-- stmt: a column the child lists is the child's own even though a parent has it
CREATE TABLE zzt9x_v1 (i int, j int);
CREATE TABLE zzt9x_v2 (i int) INHERITS (zzt9x_v1);

-- begin-expected
-- columns: attname | attnum | attislocal | attinhcount
-- row: i | 1 | t | 1
-- row: j | 2 | f | 1
-- end-expected
SELECT a.attname, a.attnum, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_v2' AND a.attnum > 0
 ORDER BY a.attnum;

-- stmt: and a column that arrives later is not
ALTER TABLE zzt9x_v1 ADD COLUMN m text;

-- begin-expected
-- columns: attname | attnum | attislocal | attinhcount
-- row: i | 1 | t | 1
-- row: j | 2 | f | 1
-- row: m | 3 | f | 1
-- end-expected
SELECT a.attname, a.attnum, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_v2' AND a.attnum > 0
 ORDER BY a.attnum;

DROP TABLE zzt9x_v2;
DROP TABLE zzt9x_v1;

-- stmt: NO INHERIT under two parents keeps the remaining parent's count
CREATE TABLE zzt9x_x0 (i int);
CREATE TABLE zzt9x_x1 (i int);
CREATE TABLE zzt9x_x2 (i int) INHERITS (zzt9x_x0, zzt9x_x1);
ALTER TABLE zzt9x_x2 NO INHERIT zzt9x_x0;

-- begin-expected
-- columns: parent
-- row: zzt9x_x1
-- end-expected
SELECT p.relname AS parent FROM pg_inherits h JOIN pg_class p ON p.oid = h.inhparent
 JOIN pg_class cl ON cl.oid = h.inhrelid WHERE cl.relname = 'zzt9x_x2' ORDER BY 1;

-- begin-expected
-- columns: attname | attislocal | attinhcount
-- row: i | t | 1
-- end-expected
SELECT a.attname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_x2' AND a.attnum > 0
 ORDER BY a.attnum;

DROP TABLE zzt9x_x2;
DROP TABLE zzt9x_x0;
DROP TABLE zzt9x_x1;

-- ============================================================================
-- ALTER TABLE ... INHERIT raises the count and NO INHERIT lowers it again
-- ============================================================================

CREATE TABLE zzt9x_ap (i int NOT NULL, CONSTRAINT zzt9x_ack CHECK (i > 0));
CREATE TABLE zzt9x_ac (i int NOT NULL, CONSTRAINT zzt9x_ack CHECK (i > 0));

-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzt9x_ac_i_not_null | n | t | 0
-- row: zzt9x_ack | c | t | 0
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_ac' ORDER BY 1;

ALTER TABLE zzt9x_ac INHERIT zzt9x_ap;

-- stmt: the child still declared both, and now a parent hands both down as well
-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzt9x_ac_i_not_null | n | t | 1
-- row: zzt9x_ack | c | t | 1
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_ac' ORDER BY 1;

-- begin-expected
-- columns: attname | attislocal | attinhcount
-- row: i | t | 1
-- end-expected
SELECT a.attname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_ac' AND a.attnum > 0
 ORDER BY a.attnum;

-- stmt: and while a parent hands it down the child may not drop it
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzt9x_ack" of relation "zzt9x_ac"
-- end-expected-error
ALTER TABLE zzt9x_ac DROP CONSTRAINT zzt9x_ack;

ALTER TABLE zzt9x_ac NO INHERIT zzt9x_ap;

-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzt9x_ac_i_not_null | n | t | 0
-- row: zzt9x_ack | c | t | 0
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_ac' ORDER BY 1;

-- stmt: with nobody handing it down the drop is allowed
ALTER TABLE zzt9x_ac DROP CONSTRAINT zzt9x_ack;

-- begin-expected
-- columns: conname
-- row: zzt9x_ac_i_not_null
-- end-expected
SELECT c.conname FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname = 'zzt9x_ac' ORDER BY 1;

DROP TABLE zzt9x_ac;
DROP TABLE zzt9x_ap;

-- ============================================================================
-- A partition may not drop what it holds for the partitioned table
-- ============================================================================

CREATE TABLE zzt9x_kp (i int, j int, CONSTRAINT zzt9x_kppk PRIMARY KEY (i),
                       CONSTRAINT zzt9x_kpck CHECK (j > 0)) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_kp0 PARTITION OF zzt9x_kp FOR VALUES FROM (0) TO (10);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzt9x_kp0_pkey" of relation "zzt9x_kp0"
-- end-expected-error
ALTER TABLE zzt9x_kp0 DROP CONSTRAINT zzt9x_kp0_pkey;

-- stmt: a partition declares nothing of its own
-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzt9x_kp0_pkey | p | f | 1
-- row: zzt9x_kp_i_not_null | n | f | 1
-- row: zzt9x_kpck | c | f | 1
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_kp0' ORDER BY 1;

-- begin-expected
-- columns: attname | attislocal | attinhcount
-- row: i | f | 1
-- row: j | f | 1
-- end-expected
SELECT a.attname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_kp0' AND a.attnum > 0
 ORDER BY a.attnum;

DROP TABLE zzt9x_kp;

-- stmt: an attached table is read the same way, and a detached one answers for itself
CREATE TABLE zzt9x_dt (i int NOT NULL, CONSTRAINT zzt9x_dtck CHECK (i > 0)) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_dt0 PARTITION OF zzt9x_dt FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: contype | conislocal | coninhcount
-- row: c | f | 1
-- row: n | f | 1
-- end-expected
SELECT c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_dt0' ORDER BY 1;

ALTER TABLE zzt9x_dt DETACH PARTITION zzt9x_dt0;

-- begin-expected
-- columns: contype | conislocal | coninhcount
-- row: c | t | 0
-- row: n | t | 0
-- end-expected
SELECT c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_dt0' ORDER BY 1;

-- begin-expected
-- columns: attname | attislocal | attinhcount
-- row: i | t | 0
-- end-expected
SELECT a.attname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_dt0' AND a.attnum > 0
 ORDER BY 1;

DROP TABLE zzt9x_dt;
DROP TABLE zzt9x_dt0;

CREATE TABLE zzt9x_at (i int NOT NULL, CONSTRAINT zzt9x_atck CHECK (i > 0)) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_at1 (i int NOT NULL, CONSTRAINT zzt9x_atck CHECK (i > 0));
ALTER TABLE zzt9x_at ATTACH PARTITION zzt9x_at1 FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: contype | conislocal | coninhcount
-- row: c | f | 1
-- row: n | f | 1
-- end-expected
SELECT c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_at1' ORDER BY 1;

-- begin-expected
-- columns: attname | attislocal | attinhcount
-- row: i | f | 1
-- end-expected
SELECT a.attname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_at1' AND a.attnum > 0
 ORDER BY 1;

DROP TABLE zzt9x_at;

-- ============================================================================
-- Dropping the parent, and dropping the parent's constraint
-- ============================================================================

CREATE TABLE zzt9x_dp (i int CONSTRAINT zzt9x_dck CHECK (i > 0));
CREATE TABLE zzt9x_dc (i int CONSTRAINT zzt9x_dck CHECK (i > 0)) INHERITS (zzt9x_dp);
CREATE TABLE zzt9x_de () INHERITS (zzt9x_dp);

-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_dc | t | 1
-- row: zzt9x_de | f | 1
-- row: zzt9x_dp | t | 0
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_dp','zzt9x_dc','zzt9x_de') AND c.contype = 'c' ORDER BY 1;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table zzt9x_dp because other objects depend on it
-- end-expected-error
DROP TABLE zzt9x_dp;

-- stmt: the refused drop moved nothing
-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_dc | t | 1
-- row: zzt9x_de | f | 1
-- row: zzt9x_dp | t | 0
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_dp','zzt9x_dc','zzt9x_de') AND c.contype = 'c' ORDER BY 1;

-- stmt: dropping the parent's constraint takes it off the child that only took it, and
-- leaves the one the other child declared with nobody handing it down
ALTER TABLE zzt9x_dp DROP CONSTRAINT zzt9x_dck;

-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_dc | t | 0
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname IN ('zzt9x_dp','zzt9x_dc','zzt9x_de') AND c.contype = 'c' ORDER BY 1;

DROP TABLE zzt9x_de;
DROP TABLE zzt9x_dc;
DROP TABLE zzt9x_dp;

-- stmt: and a parent that is let go of before it is dropped leaves the rest of the counts alone
CREATE TABLE zzt9x_q0 (i int CONSTRAINT zzt9x_qck CHECK (i > 0));
CREATE TABLE zzt9x_q1 (i int CONSTRAINT zzt9x_qck CHECK (i > 0));
CREATE TABLE zzt9x_qc (i int CONSTRAINT zzt9x_qck CHECK (i > 0)) INHERITS (zzt9x_q0, zzt9x_q1);
ALTER TABLE zzt9x_qc NO INHERIT zzt9x_q0;
DROP TABLE zzt9x_q0;

-- begin-expected
-- columns: relname | conislocal | coninhcount
-- row: zzt9x_qc | t | 1
-- end-expected
SELECT cl.relname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_qc' AND c.contype = 'c';

-- begin-expected
-- columns: relname | attislocal | attinhcount
-- row: zzt9x_qc | t | 1
-- end-expected
SELECT cl.relname, a.attislocal, a.attinhcount FROM pg_attribute a
 JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzt9x_qc' AND a.attnum > 0;

DROP TABLE zzt9x_qc;
DROP TABLE zzt9x_q1;

-- ============================================================================
-- A child that declares nothing of its own
-- ============================================================================

CREATE TABLE zzt9x_cl (a int CHECK (a > 0), b int NOT NULL, c int PRIMARY KEY);
CREATE TABLE zzt9x_clc () INHERITS (zzt9x_cl);

-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzt9x_cl_a_check | c | f | 1
-- row: zzt9x_cl_b_not_null | n | f | 1
-- row: zzt9x_cl_c_not_null | n | f | 1
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_clc' ORDER BY 1;

-- begin-expected
-- columns: conname | contype | conislocal | coninhcount
-- row: zzt9x_cl_a_check | c | t | 0
-- row: zzt9x_cl_b_not_null | n | t | 0
-- row: zzt9x_cl_c_not_null | n | t | 0
-- row: zzt9x_cl_pkey | p | t | 0
-- end-expected
SELECT c.conname, c.contype, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_cl' ORDER BY 1;

-- stmt: a rule the child adds for itself is nobody else's, and the child may drop it
ALTER TABLE zzt9x_clc ADD CONSTRAINT zzt9x_own CHECK (a < 100);

-- begin-expected
-- columns: conname | conislocal | coninhcount
-- row: zzt9x_own | t | 0
-- end-expected
SELECT c.conname, c.conislocal, c.coninhcount FROM pg_constraint c
 JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_clc' AND c.conname = 'zzt9x_own';

ALTER TABLE zzt9x_clc DROP CONSTRAINT zzt9x_own;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid
 WHERE cl.relname = 'zzt9x_clc' AND c.conname = 'zzt9x_own';

DROP TABLE zzt9x_clc;
DROP TABLE zzt9x_cl;
