-- ============================================================================
-- What a relation keeps when the parent hands a column down, and when it
-- takes it back
--
-- A column a parent drops goes only from the relations that were holding it
-- for that parent; a relation that declared the column itself keeps it, and so
-- does one another parent still hands it to. The rules on that column keep the
-- name and the count they were created with, whatever the hierarchy does
-- afterwards. Every value here was read off PostgreSQL 18.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- A column two parents declare goes only when both have let it go
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_b0 (i int, j int);
CREATE TABLE zw5x_b1 (i int, j int);
CREATE TABLE zw5x_b2 () INHERITS (zw5x_b0, zw5x_b1);
INSERT INTO zw5x_b2 VALUES (1, 2);
ALTER TABLE zw5x_b0 DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: i/false/2,j/false/1
-- end-expected
SELECT string_agg(attname || '/' || attislocal::text || '/' || attinhcount::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_b2'::regclass AND attnum > 0 AND NOT attisdropped;

-- begin-expected
-- columns: i | j
-- row: 1 | 2
-- end-expected
SELECT i, j FROM zw5x_b2 ORDER BY i;

ALTER TABLE zw5x_b1 DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: i/false/2
-- end-expected
SELECT string_agg(attname || '/' || attislocal::text || '/' || attinhcount::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_b2'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_b2;
DROP TABLE zw5x_b0;
DROP TABLE zw5x_b1;

-- ----------------------------------------------------------------------------
-- A child that listed the column itself keeps it, with what it holds in it
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_c0 (i int, j int);
CREATE TABLE zw5x_c1 (j int) INHERITS (zw5x_c0);
INSERT INTO zw5x_c1 VALUES (3, 4);
ALTER TABLE zw5x_c0 DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: i/false/1,j/true/0
-- end-expected
SELECT string_agg(attname || '/' || attislocal::text || '/' || attinhcount::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_c1'::regclass AND attnum > 0 AND NOT attisdropped;

-- begin-expected
-- columns: i | j
-- row: 3 | 4
-- end-expected
SELECT i, j FROM zw5x_c1 ORDER BY i;

-- begin-expected
-- columns: i
-- row: 3
-- end-expected
SELECT i FROM zw5x_c0 ORDER BY i;

-- Nobody hands it down any longer, so the child may now drop it itself.
ALTER TABLE zw5x_c1 DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: i
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_c1'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_c1;
DROP TABLE zw5x_c0;

-- ----------------------------------------------------------------------------
-- The drop reaches the whole chain and stops where a relation declared it
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_e0 (i int, j int);
CREATE TABLE zw5x_e1 () INHERITS (zw5x_e0);
CREATE TABLE zw5x_e2 (j int) INHERITS (zw5x_e1);
ALTER TABLE zw5x_e0 DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: zw5x_e0:i/true/0,zw5x_e1:i/false/1,zw5x_e2:i/false/1,zw5x_e2:j/true/0
-- end-expected
SELECT string_agg(c.relname || ':' || a.attname || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY c.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname IN ('zw5x_e0','zw5x_e1','zw5x_e2') AND a.attnum > 0 AND NOT a.attisdropped;

DROP TABLE zw5x_e2;
DROP TABLE zw5x_e1;
DROP TABLE zw5x_e0;

-- ----------------------------------------------------------------------------
-- CASCADE is about the objects that depend on the column, not the children
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_h0 (i int, j int);
CREATE TABLE zw5x_h1 (j int) INHERITS (zw5x_h0);
ALTER TABLE zw5x_h0 DROP COLUMN j CASCADE;

-- begin-expected
-- columns: atts
-- row: zw5x_h0:i/true/0,zw5x_h1:i/false/1,zw5x_h1:j/true/0
-- end-expected
SELECT string_agg(c.relname || ':' || a.attname || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY c.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname IN ('zw5x_h0','zw5x_h1') AND a.attnum > 0 AND NOT a.attisdropped;

DROP TABLE zw5x_h1;
DROP TABLE zw5x_h0;

-- ----------------------------------------------------------------------------
-- ONLY leaves the first generation holding the column as one of its own
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_p4 (i int, j int);
CREATE TABLE zw5x_q4 (i int, j int);
CREATE TABLE zw5x_c4 () INHERITS (zw5x_p4, zw5x_q4);
ALTER TABLE ONLY zw5x_p4 DROP COLUMN j;

-- The child is told both that the column is its own and that one parent still
-- hands it down.
-- begin-expected
-- columns: atts
-- row: i/false/2,j/true/1
-- end-expected
SELECT string_agg(attname || '/' || attislocal::text || '/' || attinhcount::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_c4'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_c4;
DROP TABLE zw5x_p4;
DROP TABLE zw5x_q4;

CREATE TABLE zw5x_y0 (i int, j int);
CREATE TABLE zw5x_y1 () INHERITS (zw5x_y0);
CREATE TABLE zw5x_y2 () INHERITS (zw5x_y1);
ALTER TABLE ONLY zw5x_y0 DROP COLUMN j;

-- Only the first generation is told anything; the grandchild goes on taking
-- the column from the child, which still declares it.
-- begin-expected
-- columns: atts
-- row: zw5x_y0:i/true/0,zw5x_y1:i/false/1,zw5x_y1:j/true/0,zw5x_y2:i/false/1,zw5x_y2:j/false/1
-- end-expected
SELECT string_agg(c.relname || ':' || a.attname || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY c.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname IN ('zw5x_y0','zw5x_y1','zw5x_y2') AND a.attnum > 0 AND NOT a.attisdropped;

ALTER TABLE zw5x_y1 DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: zw5x_y0:i,zw5x_y1:i,zw5x_y2:i
-- end-expected
SELECT string_agg(c.relname || ':' || a.attname, ',' ORDER BY c.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname IN ('zw5x_y0','zw5x_y1','zw5x_y2') AND a.attnum > 0 AND NOT a.attisdropped;

DROP TABLE zw5x_y2;
DROP TABLE zw5x_y1;
DROP TABLE zw5x_y0;

-- ----------------------------------------------------------------------------
-- A partition declares nothing of its own, however it joined
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_m0 (i int, j int) PARTITION BY RANGE (i);
CREATE TABLE zw5x_m1 PARTITION OF zw5x_m0 FOR VALUES FROM (0) TO (10);
CREATE TABLE zw5x_m2 (i int, j int);
ALTER TABLE zw5x_m0 ATTACH PARTITION zw5x_m2 FOR VALUES FROM (10) TO (20);
ALTER TABLE zw5x_m0 DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: zw5x_m0:i/true/0,zw5x_m1:i/false/1,zw5x_m2:i/false/1
-- end-expected
SELECT string_agg(c.relname || ':' || a.attname || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY c.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname IN ('zw5x_m0','zw5x_m1','zw5x_m2') AND a.attnum > 0 AND NOT a.attisdropped;

DROP TABLE zw5x_m0;

-- ----------------------------------------------------------------------------
-- A drop that rolled back leaves every relation holding what it held
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_v0 (i int, j int);
CREATE TABLE zw5x_v1 () INHERITS (zw5x_v0);
CREATE TABLE zw5x_v2 (j int) INHERITS (zw5x_v0);
INSERT INTO zw5x_v1 VALUES (1, 2);
INSERT INTO zw5x_v2 VALUES (3, 4);
BEGIN;
ALTER TABLE zw5x_v0 DROP COLUMN j;
ROLLBACK;

-- begin-expected
-- columns: atts
-- row: zw5x_v0:i/true/0,zw5x_v0:j/true/0,zw5x_v1:i/false/1,zw5x_v1:j/false/1,zw5x_v2:i/false/1,zw5x_v2:j/true/1
-- end-expected
SELECT string_agg(c.relname || ':' || a.attname || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY c.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname IN ('zw5x_v0','zw5x_v1','zw5x_v2') AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: i | j
-- row: 1 | 2
-- end-expected
SELECT i, j FROM zw5x_v1 ORDER BY i;

-- begin-expected
-- columns: i | j
-- row: 3 | 4
-- end-expected
SELECT i, j FROM zw5x_v2 ORDER BY i;

DROP TABLE zw5x_v2;
DROP TABLE zw5x_v1;
DROP TABLE zw5x_v0;

-- ----------------------------------------------------------------------------
-- The rules on a column the parent dropped keep the count they were made with
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_k0 (i int, j int NOT NULL, CONSTRAINT zw5x_kc CHECK (j > 0));
CREATE TABLE zw5x_k1 (j int NOT NULL) INHERITS (zw5x_k0);
CREATE TABLE zw5x_k2 () INHERITS (zw5x_k1);
ALTER TABLE zw5x_k0 DROP COLUMN j;

-- begin-expected
-- columns: cons
-- row: zw5x_k1_j_not_null/true/1,zw5x_kc/false/1
-- end-expected
SELECT string_agg(conname || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_k1'::regclass;

-- Neither may be withdrawn here: the count is one that nothing left standing
-- can decrement.
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zw5x_kc" of relation "zw5x_k1"
-- end-expected-error
ALTER TABLE zw5x_k1 DROP CONSTRAINT zw5x_kc;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zw5x_k1_j_not_null" of relation "zw5x_k1"
-- end-expected-error
ALTER TABLE zw5x_k1 DROP CONSTRAINT zw5x_k1_j_not_null;

-- Dropping the column takes them, and takes the descendant's with them.
ALTER TABLE zw5x_k1 DROP COLUMN j;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint WHERE conrelid IN ('zw5x_k1'::regclass, 'zw5x_k2'::regclass);

DROP TABLE zw5x_k2;
DROP TABLE zw5x_k1;
DROP TABLE zw5x_k0;

CREATE TABLE zw5x_z0 (i int, j int NOT NULL, CONSTRAINT zw5x_zc CHECK (j > 0));
CREATE TABLE zw5x_z1 (i int, j int NOT NULL, CONSTRAINT zw5x_zc CHECK (j > 0));
CREATE TABLE zw5x_z2 () INHERITS (zw5x_z0, zw5x_z1);
ALTER TABLE ONLY zw5x_z0 DROP COLUMN j;

-- begin-expected
-- columns: cons
-- row: zw5x_z0_j_not_null/false/2,zw5x_zc/false/2
-- end-expected
SELECT string_agg(conname || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_z2'::regclass;

-- begin-expected
-- columns: atts
-- row: i/false/2,j/true/1
-- end-expected
SELECT string_agg(attname || '/' || attislocal::text || '/' || attinhcount::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_z2'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_z2;
DROP TABLE zw5x_z0;
DROP TABLE zw5x_z1;

-- ----------------------------------------------------------------------------
-- A relation taken out of a hierarchy keeps its constraint's name
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_dt (i int NOT NULL, j int, CONSTRAINT zw5x_dtck CHECK (i > 0)) PARTITION BY RANGE (i);
CREATE TABLE zw5x_dt0 PARTITION OF zw5x_dt FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: cons
-- row: zw5x_dt_i_not_null/n/false/1,zw5x_dtck/c/false/1
-- end-expected
SELECT string_agg(conname || '/' || contype::text || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_dt0'::regclass;

ALTER TABLE zw5x_dt DETACH PARTITION zw5x_dt0;

-- The detached table answers to the name the partitioned table gave it, not to
-- one derived from its own.
-- begin-expected
-- columns: cons
-- row: zw5x_dt_i_not_null/n/true/0,zw5x_dtck/c/true/0
-- end-expected
SELECT string_agg(conname || '/' || contype::text || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_dt0'::regclass;

ALTER TABLE zw5x_dt0 DROP CONSTRAINT zw5x_dt_i_not_null;

-- begin-expected
-- columns: cons
-- row: zw5x_dtck
-- end-expected
SELECT string_agg(conname, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_dt0'::regclass;

DROP TABLE zw5x_dt;
DROP TABLE zw5x_dt0;

CREATE TABLE zw5x_s (i int NOT NULL, j int) PARTITION BY RANGE (i);
CREATE TABLE zw5x_s0 PARTITION OF zw5x_s FOR VALUES FROM (0) TO (10);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zw5x_s_i_not_null" of relation "zw5x_s0"
-- end-expected-error
ALTER TABLE zw5x_s0 DROP CONSTRAINT zw5x_s_i_not_null;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "zw5x_s0_i_not_null" of relation "zw5x_s0" does not exist
-- end-expected-error
ALTER TABLE zw5x_s0 DROP CONSTRAINT zw5x_s0_i_not_null;

DROP TABLE zw5x_s;

-- LIKE copies the constraint, which means the name it answers to on the source.
CREATE TABLE zw5x_u0 (i int NOT NULL, j int);
CREATE TABLE zw5x_u2 () INHERITS (zw5x_u0);
CREATE TABLE zw5x_u3 (LIKE zw5x_u2);

-- begin-expected
-- columns: cons
-- row: zw5x_u0_i_not_null/true/0
-- end-expected
SELECT string_agg(conname || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_u3'::regclass;

DROP TABLE zw5x_u3;
DROP TABLE zw5x_u2;
DROP TABLE zw5x_u0;

-- ----------------------------------------------------------------------------
-- A table joining a partitioned table carries the rules it will answer for
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_ao (i int NOT NULL, j int, CONSTRAINT zw5x_aock CHECK (j > 0)) PARTITION BY RANGE (i);
CREATE TABLE zw5x_ao1 (i int, j int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "i" in child table "zw5x_ao1" must be marked NOT NULL
-- end-expected-error
ALTER TABLE zw5x_ao ATTACH PARTITION zw5x_ao1 FOR VALUES FROM (0) TO (10);

-- The rules are read ahead of the rows: this table holds a row outside the
-- bound as well, and PostgreSQL names the missing rule.
CREATE TABLE zw5x_ao2 (i int NOT NULL, j int);
INSERT INTO zw5x_ao2 VALUES (99, 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table is missing constraint "zw5x_aock"
-- end-expected-error
ALTER TABLE zw5x_ao ATTACH PARTITION zw5x_ao2 FOR VALUES FROM (0) TO (10);

CREATE TABLE zw5x_ao6 (i int NOT NULL, j int, CONSTRAINT zw5x_aock CHECK (j > 0) NO INHERIT);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: constraint "zw5x_aock" conflicts with non-inherited constraint on child table "zw5x_ao6"
-- end-expected-error
ALTER TABLE zw5x_ao ATTACH PARTITION zw5x_ao6 FOR VALUES FROM (20) TO (30);

-- A rule of that name testing something else is a different rule.
CREATE TABLE zw5x_ao5 (i int NOT NULL, j int, CONSTRAINT zw5x_aock CHECK (0 < j));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table "zw5x_ao5" has different definition for check constraint "zw5x_aock"
-- end-expected-error
ALTER TABLE zw5x_ao ATTACH PARTITION zw5x_ao5 FOR VALUES FROM (20) TO (30);

-- A pair of parentheses is not.
CREATE TABLE zw5x_ao4 (i int NOT NULL, j int, CONSTRAINT zw5x_aock CHECK ((j) > 0));
ALTER TABLE zw5x_ao ATTACH PARTITION zw5x_ao4 FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: cons
-- row: zw5x_ao4_i_not_null/n/false/1,zw5x_aock/c/false/1
-- end-expected
SELECT string_agg(conname || '/' || contype::text || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_ao4'::regclass;

ALTER TABLE zw5x_ao DETACH PARTITION zw5x_ao4;

-- An attached table keeps its own names, unlike one created with PARTITION OF.
-- begin-expected
-- columns: cons
-- row: zw5x_ao4_i_not_null/n/true/0,zw5x_aock/c/true/0
-- end-expected
SELECT string_agg(conname || '/' || contype::text || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_ao4'::regclass;

DROP TABLE zw5x_ao;
DROP TABLE zw5x_ao1;
DROP TABLE zw5x_ao2;
DROP TABLE zw5x_ao4;
DROP TABLE zw5x_ao5;
DROP TABLE zw5x_ao6;

-- ALTER TABLE ... INHERIT answers the same way, word for word.
CREATE TABLE zw5x_ai (i int NOT NULL, j int, CONSTRAINT zw5x_aick CHECK (j > 0));
CREATE TABLE zw5x_ai1 (i int NOT NULL, j int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table is missing constraint "zw5x_aick"
-- end-expected-error
ALTER TABLE zw5x_ai1 INHERIT zw5x_ai;

CREATE TABLE zw5x_ai2 (i int NOT NULL, j int, CONSTRAINT zw5x_aick CHECK (j > 1));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table "zw5x_ai2" has different definition for check constraint "zw5x_aick"
-- end-expected-error
ALTER TABLE zw5x_ai2 INHERIT zw5x_ai;

CREATE TABLE zw5x_ai3 (i int NOT NULL, j int, CONSTRAINT zw5x_aick CHECK ((j) > 0));
ALTER TABLE zw5x_ai3 INHERIT zw5x_ai;

-- begin-expected
-- columns: cons
-- row: zw5x_ai3_i_not_null/true/1,zw5x_aick/true/1
-- end-expected
SELECT string_agg(conname || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_ai3'::regclass;

DROP TABLE zw5x_ai3;
DROP TABLE zw5x_ai2;
DROP TABLE zw5x_ai1;
DROP TABLE zw5x_ai;

-- ----------------------------------------------------------------------------
-- A rule a hierarchy cannot express is refused where it is written
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot add NO INHERIT constraint to partitioned table "zw5x_np"
-- end-expected-error
CREATE TABLE zw5x_np (i int, CONSTRAINT zw5x_npc CHECK (i > 0) NO INHERIT) PARTITION BY RANGE (i);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot add NO INHERIT constraint to partitioned table "zw5x_nq"
-- end-expected-error
CREATE TABLE zw5x_nq (i int, CHECK (i > 0) NO INHERIT) PARTITION BY LIST (i);

-- A NOT NULL saying the same thing is a different refusal.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: not-null constraints on partitioned tables cannot be NO INHERIT
-- end-expected-error
CREATE TABLE zw5x_nr (i int NOT NULL NO INHERIT) PARTITION BY RANGE (i);

CREATE TABLE zw5x_ns (i int) PARTITION BY RANGE (i);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot add NO INHERIT constraint to partitioned table "zw5x_ns"
-- end-expected-error
ALTER TABLE zw5x_ns ADD CONSTRAINT zw5x_nsc CHECK (i > 0) NO INHERIT;

DROP TABLE zw5x_ns;

-- On an ordinary table the rule can be expressed, and is recorded.
CREATE TABLE zw5x_nt (i int, CONSTRAINT zw5x_ntc CHECK (i > 0) NO INHERIT);

-- begin-expected
-- columns: cons
-- row: zw5x_ntc/true/true/0
-- end-expected
SELECT string_agg(conname || '/' || connoinherit::text || '/' || conislocal::text || '/' || coninhcount::text, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_nt'::regclass;

DROP TABLE zw5x_nt;
