-- ============================================================================
-- A NOT NULL added to a parent under a name of its own is every descendant's
-- ============================================================================
CREATE TABLE zzn6_gp (i int, j int);
CREATE TABLE zzn6_gc (i int, j int) INHERITS (zzn6_gp);
CREATE TABLE zzn6_gg () INHERITS (zzn6_gc);
CREATE TABLE zzn6_gs (i int, j int) INHERITS (zzn6_gp);
ALTER TABLE zzn6_gp ADD CONSTRAINT zzn6_gn NOT NULL i;

-- stmt: the grandchild and the second child hold it too, under the name it was made with
-- begin-expected
-- columns: cons
-- row: zzn6_gc:zzn6_gn/false/1,zzn6_gg:zzn6_gn/false/1,zzn6_gp:zzn6_gn/true/0,zzn6_gs:zzn6_gn/false/1
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_gp','zzn6_gc','zzn6_gg','zzn6_gs') AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: zzn6_gc:i/true/true/1,zzn6_gc:j/false/true/1,zzn6_gg:i/true/false/1,zzn6_gg:j/false/false/1,zzn6_gp:i/true/true/0,zzn6_gp:j/false/true/0,zzn6_gs:i/true/true/1,zzn6_gs:j/false/true/1
-- end-expected
SELECT string_agg(cl.relname || ':' || a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY cl.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname IN ('zzn6_gp','zzn6_gc','zzn6_gg','zzn6_gs') AND a.attnum > 0 AND NOT a.attisdropped;

-- stmt: and the rule is enforced two generations down and on the side branch
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_gg" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_gg (j) VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_gs" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_gs (j) VALUES (1);

INSERT INTO zzn6_gg (i, j) VALUES (1, 1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_gp;

-- stmt: it is the parent's to withdraw, so the grandchild is sent back to the parent
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzn6_gn" of relation "zzn6_gg"
-- end-expected-error
ALTER TABLE zzn6_gg ALTER COLUMN i DROP NOT NULL;

ALTER TABLE zzn6_gp DROP CONSTRAINT zzn6_gn;

-- stmt: withdrawing it reaches every relation it had reached
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_gp','zzn6_gc','zzn6_gg','zzn6_gs') AND c.contype = 'n';

INSERT INTO zzn6_gg (j) VALUES (2);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzn6_gg;

DROP TABLE zzn6_gg;
DROP TABLE zzn6_gs;
DROP TABLE zzn6_gc;
DROP TABLE zzn6_gp;

-- ============================================================================
-- The rows a descendant holds decide whether the rule can be declared at all
-- ============================================================================
CREATE TABLE zzn6_wp (i int, j int);
CREATE TABLE zzn6_wc (i int, j int) INHERITS (zzn6_wp);
INSERT INTO zzn6_wc (j) VALUES (5);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "i" of relation "zzn6_wc" contains null values
-- end-expected-error
ALTER TABLE zzn6_wp ADD CONSTRAINT zzn6_wn NOT NULL i;

-- stmt: and PostgreSQL will not store one the relations below do not take on
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraint must be added to child tables too
-- end-expected-error
ALTER TABLE ONLY zzn6_wp ADD CONSTRAINT zzn6_wn NOT NULL i;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_wp','zzn6_wc') AND c.contype = 'n';

DELETE FROM zzn6_wc;
ALTER TABLE zzn6_wp ADD CONSTRAINT zzn6_wn NOT NULL i;

-- stmt: a column that already refuses a null has nothing left for a second name to answer
--       to, so the declaration is folded into the constraint already there and creates
--       nothing; the hierarchy holds the one rule already made, under its first name
ALTER TABLE ONLY zzn6_wp ADD CONSTRAINT zzn6_wn2 NOT NULL i;

-- begin-expected
-- columns: cons
-- row: zzn6_wc:zzn6_wn/false/1,zzn6_wp:zzn6_wn/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_wp','zzn6_wc') AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_wc" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_wc (j) VALUES (5);

INSERT INTO zzn6_wc (i, j) VALUES (1, 5);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_wp;

DROP TABLE zzn6_wc;
DROP TABLE zzn6_wp;

-- ============================================================================
-- A named NOT NULL on a partitioned table reaches the leaf under a sub-partition
-- ============================================================================
CREATE TABLE zzn6_pp (i int, j int) PARTITION BY RANGE (i);
CREATE TABLE zzn6_p1 PARTITION OF zzn6_pp FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (i);
CREATE TABLE zzn6_p2 PARTITION OF zzn6_p1 FOR VALUES FROM (0) TO (5);
ALTER TABLE zzn6_pp ADD CONSTRAINT zzn6_pn NOT NULL j;

-- begin-expected
-- columns: cons
-- row: zzn6_p1:zzn6_pn/false/1,zzn6_p2:zzn6_pn/false/1,zzn6_pp:zzn6_pn/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_pp','zzn6_p1','zzn6_p2') AND c.contype = 'n';

-- stmt: the row is refused in the leaf it was routed to, and it is named
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzn6_p2" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_pp (i) VALUES (1);

INSERT INTO zzn6_pp (i, j) VALUES (1, 1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_p2;

DROP TABLE zzn6_pp;

-- ============================================================================
-- A rolled-back DROP NOT NULL leaves the hierarchy refusing what it refused
-- ============================================================================
CREATE TABLE zzn6_rp (i int NOT NULL, j int);
CREATE TABLE zzn6_rc (i int, j int) INHERITS (zzn6_rp);

BEGIN;
ALTER TABLE zzn6_rp ALTER COLUMN i DROP NOT NULL;

-- stmt: inside the transaction the child has let go of it too
-- begin-expected
-- columns: cons
-- row: NULL
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_rp','zzn6_rc') AND c.contype = 'n';

INSERT INTO zzn6_rc (j) VALUES (99);
ROLLBACK;

-- begin-expected
-- columns: cons
-- row: zzn6_rc:zzn6_rp_i_not_null/false/1,zzn6_rp:zzn6_rp_i_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_rp','zzn6_rc') AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/true/true/1,j/false/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzn6_rc' AND a.attnum > 0 AND NOT a.attisdropped;

-- stmt: the row written while the rule was gone went with the rollback
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzn6_rc;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_rc" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_rc (j) VALUES (1);

-- stmt: the named spelling of the drop is undone the same way
BEGIN;
ALTER TABLE zzn6_rp DROP CONSTRAINT zzn6_rp_i_not_null;
ROLLBACK;

-- begin-expected
-- columns: cons
-- row: zzn6_rc:zzn6_rp_i_not_null/false/1,zzn6_rp:zzn6_rp_i_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_rp','zzn6_rc') AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_rc" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_rc (j) VALUES (1);

-- stmt: and so is a savepoint rolled back inside a transaction that goes on to commit
BEGIN;
SAVEPOINT zzn6_s;
ALTER TABLE zzn6_rp ALTER COLUMN i DROP NOT NULL;
ROLLBACK TO SAVEPOINT zzn6_s;
COMMIT;

-- begin-expected
-- columns: cons
-- row: zzn6_rc:zzn6_rp_i_not_null/false/1,zzn6_rp:zzn6_rp_i_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_rp','zzn6_rc') AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_rc" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_rc (j) VALUES (1);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzn6_rc;

DROP TABLE zzn6_rc;
DROP TABLE zzn6_rp;

-- ============================================================================
-- A rolled-back SET NOT NULL leaves the column taking a null again
-- ============================================================================
CREATE TABLE zzn6_sp (i int, j int);
CREATE TABLE zzn6_sc (i int, j int) INHERITS (zzn6_sp);

BEGIN;
ALTER TABLE zzn6_sp ALTER COLUMN i SET NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzn6_sc:zzn6_sp_i_not_null/false/1,zzn6_sp:zzn6_sp_i_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_sp','zzn6_sc') AND c.contype = 'n';

ROLLBACK;

-- begin-expected
-- columns: cons
-- row: NULL
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_sp','zzn6_sc') AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/false/true/1,j/false/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzn6_sc' AND a.attnum > 0 AND NOT a.attisdropped;

INSERT INTO zzn6_sc (j) VALUES (1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_sc;

-- stmt: a rolled-back ADD CONSTRAINT is undone on the child as well
BEGIN;
ALTER TABLE zzn6_sp ADD CONSTRAINT zzn6_sn NOT NULL j;
ROLLBACK;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_sp','zzn6_sc') AND c.contype = 'n';

INSERT INTO zzn6_sc (i) VALUES (2);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzn6_sc;

DROP TABLE zzn6_sc;
DROP TABLE zzn6_sp;

-- ============================================================================
-- A merged NOT NULL keeps the name it was made with when one parent lets go
-- ============================================================================
CREATE TABLE zzn6_z1 (i int NOT NULL);
CREATE TABLE zzn6_z2 (i int NOT NULL);
CREATE TABLE zzn6_z0 () INHERITS (zzn6_z1, zzn6_z2);

-- begin-expected
-- columns: cons
-- row: zzn6_z1_i_not_null/false/2
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzn6_z0' AND c.contype = 'n';

ALTER TABLE zzn6_z1 ALTER COLUMN i DROP NOT NULL;

-- stmt: the count falls by one and the name stays the one the constraint was created with
-- begin-expected
-- columns: cons
-- row: zzn6_z1_i_not_null/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzn6_z0' AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/true/false/2
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzn6_z0' AND a.attnum > 0 AND NOT a.attisdropped;

-- stmt: the other parent still declares it, so the child may not withdraw it and still refuses
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzn6_z1_i_not_null" of relation "zzn6_z0"
-- end-expected-error
ALTER TABLE zzn6_z0 ALTER COLUMN i DROP NOT NULL;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_z0" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_z0 VALUES (NULL);

INSERT INTO zzn6_z1 VALUES (NULL);
ALTER TABLE zzn6_z2 ALTER COLUMN i DROP NOT NULL;

-- stmt: with the last parent letting go, nothing is left on the child at all
-- begin-expected
-- columns: cons
-- row: NULL
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzn6_z0' AND c.contype = 'n';

INSERT INTO zzn6_z0 VALUES (NULL);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_z0;

DROP TABLE zzn6_z0;
DROP TABLE zzn6_z1;
DROP TABLE zzn6_z2;

-- ============================================================================
-- A partition that declared NOT NULL for itself holds a rule of its own
-- ============================================================================
CREATE TABLE zzn6_mp (i int, j int) PARTITION BY RANGE (i);
CREATE TABLE zzn6_ma PARTITION OF zzn6_mp FOR VALUES FROM (0) TO (10);
ALTER TABLE zzn6_ma ALTER COLUMN j SET NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzn6_ma:zzn6_ma_j_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_mp','zzn6_ma') AND c.contype = 'n';

-- stmt: the partitioned table declares nothing, and the partition refuses the row all the same
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzn6_ma" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_mp (i) VALUES (1);

ALTER TABLE zzn6_mp ALTER COLUMN j SET NOT NULL;

-- stmt: the partitioned table taking it on adds a count and leaves the rule the partition's own
-- begin-expected
-- columns: cons
-- row: zzn6_ma:zzn6_ma_j_not_null/true/1,zzn6_mp:zzn6_mp_j_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_mp','zzn6_ma') AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/false/false/1,j/true/false/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzn6_ma' AND a.attnum > 0 AND NOT a.attisdropped;

-- stmt: while the partitioned table declares it, the partition is refused in words of its own
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "j" is marked NOT NULL in parent table
-- end-expected-error
ALTER TABLE zzn6_ma ALTER COLUMN j DROP NOT NULL;

ALTER TABLE zzn6_mp ALTER COLUMN j DROP NOT NULL;

-- stmt: the partitioned table letting go takes away the count, not the rule
-- begin-expected
-- columns: cons
-- row: zzn6_ma:zzn6_ma_j_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_mp','zzn6_ma') AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzn6_ma" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_mp (i) VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzn6_ma" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_ma (i) VALUES (2);

-- stmt: it is the partition's own, so the partition may withdraw it now
ALTER TABLE zzn6_ma ALTER COLUMN j DROP NOT NULL;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzn6_ma' AND c.contype = 'n';

INSERT INTO zzn6_mp (i) VALUES (3);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_ma;

DROP TABLE zzn6_mp;

-- ============================================================================
-- A table attached to a partitioned table holds its rule under its own name
-- ============================================================================
CREATE TABLE zzn6_qp (i int, j int NOT NULL) PARTITION BY RANGE (i);
CREATE TABLE zzn6_q1 PARTITION OF zzn6_qp FOR VALUES FROM (0) TO (10);
CREATE TABLE zzn6_q2 (i int, j int NOT NULL);
ALTER TABLE zzn6_qp ATTACH PARTITION zzn6_q2 FOR VALUES FROM (10) TO (20);

-- stmt: the attached table keeps the name it made the rule with and stops owning it; the
--       partition created below the partitioned table answers to that table's name
-- begin-expected
-- columns: cons
-- row: zzn6_q1:zzn6_qp_j_not_null/false/1,zzn6_q2:zzn6_q2_j_not_null/false/1,zzn6_qp:zzn6_qp_j_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzn6_qp','zzn6_q1','zzn6_q2') AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzn6_q2" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_qp (i) VALUES (11);

-- stmt: a table with no such rule cannot join a hierarchy that has one
CREATE TABLE zzn6_q3 (i int, j int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "j" in child table "zzn6_q3" must be marked NOT NULL
-- end-expected-error
ALTER TABLE zzn6_qp ATTACH PARTITION zzn6_q3 FOR VALUES FROM (20) TO (30);

ALTER TABLE zzn6_qp DETACH PARTITION zzn6_q2;

-- stmt: detached, the rule is the table's own again, under the name it always answered to
-- begin-expected
-- columns: cons
-- row: zzn6_q2_j_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzn6_q2' AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzn6_q2" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_q2 (i) VALUES (11);

ALTER TABLE zzn6_q2 ALTER COLUMN j DROP NOT NULL;
INSERT INTO zzn6_q2 (i) VALUES (11);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_q2;

DROP TABLE zzn6_q3;
DROP TABLE zzn6_q2;
DROP TABLE zzn6_qp;

-- ============================================================================
-- A NOT NULL written NO INHERIT cannot stand beside one taken from a parent
-- ============================================================================
CREATE TABLE zzn6_hp (i int NOT NULL, j int);
CREATE TABLE zzn6_hq (i int, j int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot define not-null constraint with NO INHERIT on column "i"
-- end-expected-error
CREATE TABLE zzn6_hc (i int NOT NULL NO INHERIT, j int) INHERITS (zzn6_hp);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_class WHERE relname = 'zzn6_hc';

-- stmt: only where a parent hands that column's rule down: NO INHERIT elsewhere is taken
CREATE TABLE zzn6_h1 (i int NOT NULL NO INHERIT, j int) INHERITS (zzn6_hq);
CREATE TABLE zzn6_h2 (j int NOT NULL NO INHERIT) INHERITS (zzn6_hp);

-- begin-expected
-- columns: cons
-- row: zzn6_h2_j_not_null/true/0/true,zzn6_hp_i_not_null/false/1/false
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text || '/' || c.connoinherit::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzn6_h2' AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_h2" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_h2 (j) VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzn6_h2" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_h2 (i) VALUES (1);

INSERT INTO zzn6_h2 (i, j) VALUES (1, 1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_h2;

-- stmt: the same contradiction reached by ALTER TABLE ... INHERIT
CREATE TABLE zzn6_hd (i int NOT NULL NO INHERIT, j int);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: constraint "zzn6_hd_i_not_null" conflicts with non-inherited constraint on child table "zzn6_hd"
-- end-expected-error
ALTER TABLE zzn6_hd INHERIT zzn6_hp;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_inherits h JOIN pg_class cl ON cl.oid = h.inhrelid WHERE cl.relname = 'zzn6_hd';

-- stmt: a parent that declares nothing of the kind is joined without complaint
ALTER TABLE zzn6_hd INHERIT zzn6_hq;

-- begin-expected
-- columns: cons
-- row: zzn6_hd_i_not_null/true/0/true
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text || '/' || c.connoinherit::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzn6_hd' AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzn6_hd" violates not-null constraint
-- end-expected-error
INSERT INTO zzn6_hd (j) VALUES (1);

INSERT INTO zzn6_hd (i) VALUES (1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzn6_hd;

DROP TABLE zzn6_hd;
DROP TABLE zzn6_h2;
DROP TABLE zzn6_h1;
DROP TABLE zzn6_hq;
DROP TABLE zzn6_hp;

-- ============================================================================
-- A table belongs to one partitioned table only
-- ============================================================================
CREATE TABLE zzn6_pt (i int) PARTITION BY RANGE (i);
CREATE TABLE zzn6_pu (i int) PARTITION BY RANGE (i);
CREATE TABLE zzn6_pb (i int);
ALTER TABLE zzn6_pt ATTACH PARTITION zzn6_pb FOR VALUES FROM (0) TO (10);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzn6_pb" is already a partition
-- end-expected-error
ALTER TABLE zzn6_pt ATTACH PARTITION zzn6_pb FOR VALUES FROM (10) TO (20);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzn6_pb" is already a partition
-- end-expected-error
ALTER TABLE zzn6_pu ATTACH PARTITION zzn6_pb FOR VALUES FROM (0) TO (10);

DROP TABLE zzn6_pu;
DROP TABLE zzn6_pt;
