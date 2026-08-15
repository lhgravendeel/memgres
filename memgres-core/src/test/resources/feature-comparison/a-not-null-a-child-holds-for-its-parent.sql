-- ============================================================================
-- A child that re-lists an inherited column may add to it, never loosen it
-- ============================================================================
CREATE TABLE zzy8b1_p (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_c (i int NOT NULL, j int, k int NOT NULL) INHERITS (zzy8b1_p);

-- stmt: j was listed without NOT NULL and keeps the parent's rule all the same
-- begin-expected
-- columns: atts
-- row: i/true/true/1,j/true/true/1,k/true/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_c' AND a.attnum > 0 AND NOT a.attisdropped;

-- stmt: the restated one is the child's own and counts the parent too; the one only
-- stmt: taken answers to the parent's name; the one nobody above declares counts nobody
-- begin-expected
-- columns: cons
-- row: zzy8b1_c_i_not_null/n/true/1/false,zzy8b1_c_k_not_null/n/true/0/false,zzy8b1_p_j_not_null/n/false/1/false
-- end-expected
SELECT string_agg(c.conname || '/' || c.contype::text || '/' || c.conislocal::text || '/' || c.coninhcount::text || '/' || c.connoinherit::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_c' AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_c" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_c (i, k) VALUES (1, 1);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzy8b1_c;

INSERT INTO zzy8b1_c (i, j, k) VALUES (1, 2, 3);

-- stmt: the rule is about every row the relation holds, not only the ones written into it
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_c" violates not-null constraint
-- end-expected-error
UPDATE zzy8b1_c SET j = NULL;

-- begin-expected
-- columns: j
-- row: 2
-- end-expected
SELECT j FROM zzy8b1_c;

-- stmt: the rule is the parent's, so the child may not withdraw it
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_p_j_not_null" of relation "zzy8b1_c"
-- end-expected-error
ALTER TABLE zzy8b1_c ALTER COLUMN j DROP NOT NULL;

-- stmt: nor the one it declared while a parent goes on declaring it too
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_c_i_not_null" of relation "zzy8b1_c"
-- end-expected-error
ALTER TABLE zzy8b1_c ALTER COLUMN i DROP NOT NULL;

-- stmt: the named spelling of the drop answers the same way
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_p_j_not_null" of relation "zzy8b1_c"
-- end-expected-error
ALTER TABLE zzy8b1_c DROP CONSTRAINT zzy8b1_p_j_not_null;

-- stmt: a rule no parent declares is the child's to withdraw
ALTER TABLE zzy8b1_c ALTER COLUMN k DROP NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_c_i_not_null/true/1,zzy8b1_p_j_not_null/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_c' AND c.contype = 'n';

DROP TABLE zzy8b1_c;
DROP TABLE zzy8b1_p;

-- ============================================================================
-- The count is one per parent that declares the column NOT NULL
-- ============================================================================
CREATE TABLE zzy8b1_q0 (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_q1 (i int NOT NULL, j int, k int NOT NULL);
CREATE TABLE zzy8b1_qc (i int, j int NOT NULL, k int) INHERITS (zzy8b1_q0, zzy8b1_q1);

-- stmt: i is declared by both parents and counts two; j and k by one each
-- begin-expected
-- columns: atts
-- row: i/true/true/2,j/true/true/2,k/true/true/2
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_qc' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: cons
-- row: zzy8b1_q0_i_not_null/false/2,zzy8b1_q1_k_not_null/false/1,zzy8b1_qc_j_not_null/true/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_qc' AND c.contype = 'n';

-- stmt: a grandchild that re-lists them all takes each rule under the name it already has
CREATE TABLE zzy8b1_qg (i int, j int, k int) INHERITS (zzy8b1_qc);

-- begin-expected
-- columns: atts
-- row: i/true/true/1,j/true/true/1,k/true/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_qg' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: cons
-- row: zzy8b1_q0_i_not_null/false/1,zzy8b1_q1_k_not_null/false/1,zzy8b1_qc_j_not_null/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_qg' AND c.contype = 'n';

-- stmt: two generations down, k is still refused
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "k" of relation "zzy8b1_qg" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_qg (i, j) VALUES (1, 1);

INSERT INTO zzy8b1_qg (i, j, k) VALUES (1, 1, 1);

-- begin-expected
-- columns: c1, c2, c3
-- row: 1, 1, 1
-- end-expected
SELECT (SELECT count(*) FROM zzy8b1_qg) AS c1, (SELECT count(*) FROM zzy8b1_q0) AS c2, (SELECT count(*) FROM zzy8b1_q1) AS c3;

-- stmt: the child declared j itself, and one parent declares it too
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_qc_j_not_null" of relation "zzy8b1_qc"
-- end-expected-error
ALTER TABLE zzy8b1_qc ALTER COLUMN j DROP NOT NULL;

-- stmt: the grandchild is refused under the name the rule has carried since it was made
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_q1_k_not_null" of relation "zzy8b1_qg"
-- end-expected-error
ALTER TABLE zzy8b1_qg ALTER COLUMN k DROP NOT NULL;

DROP TABLE zzy8b1_qg;
DROP TABLE zzy8b1_qc;
DROP TABLE zzy8b1_q0;
DROP TABLE zzy8b1_q1;

-- ============================================================================
-- An explicit NULL on the child does not take the parent's rule off either
-- ============================================================================
CREATE TABLE zzy8b1_np (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_nc (i int NULL, j int NULL, k int NULL) INHERITS (zzy8b1_np);

-- begin-expected
-- columns: atts
-- row: i/true/true/1,j/true/true/1,k/false/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_nc' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: cons
-- row: zzy8b1_np_i_not_null/false/1,zzy8b1_np_j_not_null/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_nc' AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_nc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_nc (i, k) VALUES (1, 1);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzy8b1_nc;

DROP TABLE zzy8b1_nc;
DROP TABLE zzy8b1_np;

-- ============================================================================
-- A rule written NO INHERIT is about the declaring relation's rows alone
-- ============================================================================
CREATE TABLE zzy8b1_xp (i int NOT NULL NO INHERIT, j int NOT NULL);
CREATE TABLE zzy8b1_xc () INHERITS (zzy8b1_xp);
CREATE TABLE zzy8b1_xr (i int, j int) INHERITS (zzy8b1_xp);

-- begin-expected
-- columns: cons
-- row: zzy8b1_xp_i_not_null/true/0/true,zzy8b1_xp_j_not_null/true/0/false
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text || '/' || c.connoinherit::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_xp' AND c.contype = 'n';

-- stmt: neither the child that took the column nor the one that listed it holds the rule
-- begin-expected
-- columns: atts
-- row: i/false/false/1,j/true/false/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_xc' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: atts
-- row: i/false/true/1,j/true/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_xr' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: cons
-- row: zzy8b1_xp_j_not_null/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_xc' AND c.contype = 'n';

-- stmt: the descendants take the null the declaring relation is refused
INSERT INTO zzy8b1_xc (j) VALUES (1);
INSERT INTO zzy8b1_xr (j) VALUES (1);

-- begin-expected
-- columns: c1, c2
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM zzy8b1_xc WHERE i IS NULL) AS c1, (SELECT count(*) FROM zzy8b1_xr WHERE i IS NULL) AS c2;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzy8b1_xp" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_xp (j) VALUES (1);

-- stmt: the rule the parent hands down is enforced on the child all the same
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_xc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_xc (i) VALUES (1);

DROP TABLE zzy8b1_xr;
DROP TABLE zzy8b1_xc;
DROP TABLE zzy8b1_xp;

-- ============================================================================
-- A child that declared NOT NULL beside a NO INHERIT parent owns it outright
-- ============================================================================
CREATE TABLE zzy8b1_yp (i int NOT NULL NO INHERIT, j int);
CREATE TABLE zzy8b1_yc (i int NOT NULL, j int) INHERITS (zzy8b1_yp);

-- stmt: the parent's rule reaches nobody, so the child counts no parent for its own
-- begin-expected
-- columns: cons
-- row: zzy8b1_yc_i_not_null/true/0/false
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text || '/' || c.connoinherit::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_yc' AND c.contype = 'n';

ALTER TABLE zzy8b1_yc ALTER COLUMN i DROP NOT NULL;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_yc' AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/false/true/1,j/false/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_yc' AND a.attnum > 0 AND NOT a.attisdropped;

INSERT INTO zzy8b1_yc (j) VALUES (1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzy8b1_yc;

DROP TABLE zzy8b1_yc;
DROP TABLE zzy8b1_yp;

-- ============================================================================
-- A rule the parent did not declare, and then does
-- ============================================================================
CREATE TABLE zzy8b1_gp (i int NOT NULL, k int);
CREATE TABLE zzy8b1_gc (k int NOT NULL) INHERITS (zzy8b1_gp);

-- begin-expected
-- columns: cons
-- row: zzy8b1_gc_k_not_null/true/0,zzy8b1_gp_i_not_null/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_gc' AND c.contype = 'n';

-- stmt: the parent takes the rule on and the child's own now counts it
ALTER TABLE zzy8b1_gp ALTER COLUMN k SET NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_gc:zzy8b1_gc_k_not_null/true/1,zzy8b1_gc:zzy8b1_gp_i_not_null/false/1,zzy8b1_gp:zzy8b1_gp_i_not_null/true/0,zzy8b1_gp:zzy8b1_gp_k_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_gp','zzy8b1_gc') AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_gc_k_not_null" of relation "zzy8b1_gc"
-- end-expected-error
ALTER TABLE zzy8b1_gc ALTER COLUMN k DROP NOT NULL;

ALTER TABLE zzy8b1_gp ALTER COLUMN k DROP NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_gc:zzy8b1_gc_k_not_null/true/0,zzy8b1_gc:zzy8b1_gp_i_not_null/false/1,zzy8b1_gp:zzy8b1_gp_i_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_gp','zzy8b1_gc') AND c.contype = 'n';

-- stmt: the child goes on refusing what the parent now accepts
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "k" of relation "zzy8b1_gc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_gc (i) VALUES (1);

INSERT INTO zzy8b1_gp (i) VALUES (1);

-- begin-expected
-- columns: c1, c2
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*) FROM zzy8b1_gc) AS c1, (SELECT count(*) FROM zzy8b1_gp) AS c2;

DROP TABLE zzy8b1_gc;
DROP TABLE zzy8b1_gp;

-- ============================================================================
-- A parent letting go leaves standing whatever the descendant declared itself
-- ============================================================================
CREATE TABLE zzy8b1_dp (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_dc (i int NOT NULL, j int, k int NOT NULL) INHERITS (zzy8b1_dp);
CREATE TABLE zzy8b1_dg () INHERITS (zzy8b1_dc);

ALTER TABLE zzy8b1_dp ALTER COLUMN i DROP NOT NULL;

-- begin-expected
-- columns: atts
-- row: zzy8b1_dc:i/true/true/1,zzy8b1_dc:j/true/true/1,zzy8b1_dc:k/true/true/1,zzy8b1_dg:i/true/false/1,zzy8b1_dg:j/true/false/1,zzy8b1_dg:k/true/false/1,zzy8b1_dp:i/false/true/0,zzy8b1_dp:j/true/true/0,zzy8b1_dp:k/false/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY cl.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname IN ('zzy8b1_dp','zzy8b1_dc','zzy8b1_dg') AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: cons
-- row: zzy8b1_dc:zzy8b1_dc_i_not_null/true/0,zzy8b1_dc:zzy8b1_dc_k_not_null/true/0,zzy8b1_dc:zzy8b1_dp_j_not_null/false/1,zzy8b1_dg:zzy8b1_dc_i_not_null/false/1,zzy8b1_dg:zzy8b1_dc_k_not_null/false/1,zzy8b1_dg:zzy8b1_dp_j_not_null/false/1,zzy8b1_dp:zzy8b1_dp_j_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_dp','zzy8b1_dc','zzy8b1_dg') AND c.contype = 'n';

-- stmt: the parent takes the null it no longer refuses
INSERT INTO zzy8b1_dp (j) VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzy8b1_dc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_dc (j, k) VALUES (1, 1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzy8b1_dg" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_dg (j, k) VALUES (1, 1);

-- stmt: nothing above it declares the rule any more, so the child may withdraw it
ALTER TABLE zzy8b1_dc ALTER COLUMN i DROP NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_dc:zzy8b1_dc_k_not_null/true/0,zzy8b1_dc:zzy8b1_dp_j_not_null/false/1,zzy8b1_dg:zzy8b1_dc_k_not_null/false/1,zzy8b1_dg:zzy8b1_dp_j_not_null/false/1
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_dc','zzy8b1_dg') AND c.contype = 'n';

-- stmt: and the withdrawal reaches the grandchild
INSERT INTO zzy8b1_dg (j, k) VALUES (1, 1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzy8b1_dg;

DROP TABLE zzy8b1_dg;
DROP TABLE zzy8b1_dc;
DROP TABLE zzy8b1_dp;

-- ============================================================================
-- ONLY leaves the first generation holding the rule under the name it has
-- ============================================================================
CREATE TABLE zzy8b1_op (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_oc (i int NOT NULL, j int, k int NOT NULL) INHERITS (zzy8b1_op);
CREATE TABLE zzy8b1_og () INHERITS (zzy8b1_oc);

ALTER TABLE ONLY zzy8b1_op ALTER COLUMN j DROP NOT NULL;
ALTER TABLE ONLY zzy8b1_op ALTER COLUMN i DROP NOT NULL;

-- begin-expected
-- columns: atts
-- row: zzy8b1_oc:i/true/true/1,zzy8b1_oc:j/true/true/1,zzy8b1_oc:k/true/true/1,zzy8b1_og:i/true/false/1,zzy8b1_og:j/true/false/1,zzy8b1_og:k/true/false/1,zzy8b1_op:i/false/true/0,zzy8b1_op:j/false/true/0,zzy8b1_op:k/false/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY cl.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname IN ('zzy8b1_op','zzy8b1_oc','zzy8b1_og') AND a.attnum > 0 AND NOT a.attisdropped;

-- stmt: j is the child's own from now on, and keeps the name the parent gave it
-- begin-expected
-- columns: cons
-- row: zzy8b1_oc:zzy8b1_oc_i_not_null/true/0,zzy8b1_oc:zzy8b1_oc_k_not_null/true/0,zzy8b1_oc:zzy8b1_op_j_not_null/true/0,zzy8b1_og:zzy8b1_oc_i_not_null/false/1,zzy8b1_og:zzy8b1_oc_k_not_null/false/1,zzy8b1_og:zzy8b1_op_j_not_null/false/1
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_op','zzy8b1_oc','zzy8b1_og') AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_oc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_oc (i, k) VALUES (1, 1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_og" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_og (i, k) VALUES (1, 1);

-- stmt: the child owns it now, and withdrawing it reaches the grandchild
ALTER TABLE zzy8b1_oc ALTER COLUMN j DROP NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_oc:zzy8b1_oc_i_not_null/true/0,zzy8b1_oc:zzy8b1_oc_k_not_null/true/0,zzy8b1_og:zzy8b1_oc_i_not_null/false/1,zzy8b1_og:zzy8b1_oc_k_not_null/false/1
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_oc','zzy8b1_og') AND c.contype = 'n';

INSERT INTO zzy8b1_og (i, k) VALUES (1, 1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzy8b1_og;

DROP TABLE zzy8b1_og;
DROP TABLE zzy8b1_oc;
DROP TABLE zzy8b1_op;

-- ============================================================================
-- ONLY on an inheritance parent, then the child leaves the hierarchy
-- ============================================================================
CREATE TABLE zzy8b1_wp (i int, j int NOT NULL);
CREATE TABLE zzy8b1_wc (i int, j int) INHERITS (zzy8b1_wp);

ALTER TABLE ONLY zzy8b1_wp ALTER COLUMN j DROP NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_wp_j_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_wc' AND c.contype = 'n';

ALTER TABLE zzy8b1_wc NO INHERIT zzy8b1_wp;

-- stmt: leaving the hierarchy leaves the rule, and the name it was given, standing
-- begin-expected
-- columns: cons
-- row: zzy8b1_wp_j_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_wc' AND c.contype = 'n';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_wc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_wc (i) VALUES (1);

ALTER TABLE zzy8b1_wc ALTER COLUMN j DROP NOT NULL;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_wc' AND c.contype = 'n';

INSERT INTO zzy8b1_wc (i) VALUES (1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzy8b1_wc;

DROP TABLE zzy8b1_wc;
DROP TABLE zzy8b1_wp;

-- ============================================================================
-- The named spelling of the drop answers the same way, ONLY and all
-- ============================================================================
CREATE TABLE zzy8b1_np2 (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_nc2 (i int NOT NULL, j int, k int NOT NULL) INHERITS (zzy8b1_np2);
CREATE TABLE zzy8b1_ng2 () INHERITS (zzy8b1_nc2);

ALTER TABLE ONLY zzy8b1_np2 DROP CONSTRAINT zzy8b1_np2_j_not_null;

-- begin-expected
-- columns: cons
-- row: zzy8b1_nc2:zzy8b1_nc2_i_not_null/true/1,zzy8b1_nc2:zzy8b1_nc2_k_not_null/true/0,zzy8b1_nc2:zzy8b1_np2_j_not_null/true/0,zzy8b1_ng2:zzy8b1_nc2_i_not_null/false/1,zzy8b1_ng2:zzy8b1_nc2_k_not_null/false/1,zzy8b1_ng2:zzy8b1_np2_j_not_null/false/1,zzy8b1_np2:zzy8b1_np2_i_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_np2','zzy8b1_nc2','zzy8b1_ng2') AND c.contype = 'n';

-- stmt: without ONLY it reaches the descendants and leaves their own standing
ALTER TABLE zzy8b1_np2 DROP CONSTRAINT zzy8b1_np2_i_not_null;

-- begin-expected
-- columns: cons
-- row: zzy8b1_nc2:zzy8b1_nc2_i_not_null/true/0,zzy8b1_nc2:zzy8b1_nc2_k_not_null/true/0,zzy8b1_nc2:zzy8b1_np2_j_not_null/true/0,zzy8b1_ng2:zzy8b1_nc2_i_not_null/false/1,zzy8b1_ng2:zzy8b1_nc2_k_not_null/false/1,zzy8b1_ng2:zzy8b1_np2_j_not_null/false/1
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_np2','zzy8b1_nc2','zzy8b1_ng2') AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: zzy8b1_nc2:i/true,zzy8b1_nc2:j/true,zzy8b1_nc2:k/true,zzy8b1_ng2:i/true,zzy8b1_ng2:j/true,zzy8b1_ng2:k/true,zzy8b1_np2:i/false,zzy8b1_np2:j/false,zzy8b1_np2:k/false
-- end-expected
SELECT string_agg(cl.relname || ':' || a.attname || '/' || a.attnotnull::text, ',' ORDER BY cl.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname IN ('zzy8b1_np2','zzy8b1_nc2','zzy8b1_ng2') AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_ng2" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_ng2 (i, k) VALUES (1, 1);

INSERT INTO zzy8b1_np2 (i) VALUES (1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzy8b1_np2;

DROP TABLE zzy8b1_ng2;
DROP TABLE zzy8b1_nc2;
DROP TABLE zzy8b1_np2;

-- ============================================================================
-- ALTER TABLE ... INHERIT asks the child to be no less strict already
-- ============================================================================
CREATE TABLE zzy8b1_ip (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_ic (i int NOT NULL, j int, k int NOT NULL);
INSERT INTO zzy8b1_ic (i, k) VALUES (1, 1);

-- stmt: the INHERITS clause merges the parent's rule in; this refuses instead
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "j" in child table "zzy8b1_ic" must be marked NOT NULL
-- end-expected-error
ALTER TABLE zzy8b1_ic INHERIT zzy8b1_ip;

-- stmt: the refusal leaves the relation as it found it
-- begin-expected
-- columns: atts
-- row: i/true/true/0,j/false/true/0,k/true/true/0
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_ic' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_inherits h JOIN pg_class cl ON cl.oid = h.inhrelid WHERE cl.relname = 'zzy8b1_ic';

DELETE FROM zzy8b1_ic;
ALTER TABLE zzy8b1_ic ALTER COLUMN j SET NOT NULL;
ALTER TABLE zzy8b1_ic INHERIT zzy8b1_ip;

-- begin-expected
-- columns: cons
-- row: zzy8b1_ic_i_not_null/true/1,zzy8b1_ic_j_not_null/true/1,zzy8b1_ic_k_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_ic' AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/true/true/1,j/true/true/1,k/true/true/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_ic' AND a.attnum > 0 AND NOT a.attisdropped;

-- stmt: the rule it declared for itself is now a rule a parent declares as well
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_ic_j_not_null" of relation "zzy8b1_ic"
-- end-expected-error
ALTER TABLE zzy8b1_ic ALTER COLUMN j DROP NOT NULL;

ALTER TABLE zzy8b1_ic NO INHERIT zzy8b1_ip;

-- begin-expected
-- columns: cons
-- row: zzy8b1_ic_i_not_null/true/0,zzy8b1_ic_j_not_null/true/0,zzy8b1_ic_k_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_ic' AND c.contype = 'n';

ALTER TABLE zzy8b1_ic ALTER COLUMN j DROP NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_ic_i_not_null/true/0,zzy8b1_ic_k_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_ic' AND c.contype = 'n';

DROP TABLE zzy8b1_ic;
DROP TABLE zzy8b1_ip;

-- ============================================================================
-- A parent that takes the rule on later reaches every child it has
-- ============================================================================
CREATE TABLE zzy8b1_sp (i int, j int);
CREATE TABLE zzy8b1_sc (i int, j int) INHERITS (zzy8b1_sp);
INSERT INTO zzy8b1_sc (j) VALUES (1);

-- stmt: the rows of every descendant are read before the rule is taken on
-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "i" of relation "zzy8b1_sc" contains null values
-- end-expected-error
ALTER TABLE zzy8b1_sp ALTER COLUMN i SET NOT NULL;

DELETE FROM zzy8b1_sc;
ALTER TABLE zzy8b1_sp ALTER COLUMN i SET NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_sc:zzy8b1_sp_i_not_null/false/1,zzy8b1_sp:zzy8b1_sp_i_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_sp','zzy8b1_sc') AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: zzy8b1_sc:i/true/true/1,zzy8b1_sc:j/false/true/1,zzy8b1_sp:i/true/true/0,zzy8b1_sp:j/false/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY cl.relname, a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname IN ('zzy8b1_sp','zzy8b1_sc') AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "zzy8b1_sc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_sc (j) VALUES (1);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_sp_i_not_null" of relation "zzy8b1_sc"
-- end-expected-error
ALTER TABLE zzy8b1_sc ALTER COLUMN i DROP NOT NULL;

-- stmt: ONLY takes the rule on for the relation it names and for nobody below
ALTER TABLE ONLY zzy8b1_sp ALTER COLUMN j SET NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_sc:zzy8b1_sp_i_not_null/false/1,zzy8b1_sp:zzy8b1_sp_i_not_null/true/0,zzy8b1_sp:zzy8b1_sp_j_not_null/true/0
-- end-expected
SELECT string_agg(cl.relname || ':' || c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY cl.relname, c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN ('zzy8b1_sp','zzy8b1_sc') AND c.contype = 'n';

DROP TABLE zzy8b1_sc;
DROP TABLE zzy8b1_sp;

-- ============================================================================
-- A partition declares nothing of its own, and ATTACH asks the same of it
-- ============================================================================
CREATE TABLE zzy8b1_pt (i int NOT NULL, j int NOT NULL, k int) PARTITION BY RANGE (i);
CREATE TABLE zzy8b1_pa PARTITION OF zzy8b1_pt FOR VALUES FROM (1) TO (10);

-- begin-expected
-- columns: atts
-- row: i/true/false/1,j/true/false/1,k/false/false/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_pa' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: cons
-- row: zzy8b1_pt_i_not_null/false/1/false,zzy8b1_pt_j_not_null/false/1/false
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text || '/' || c.connoinherit::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_pa' AND c.contype = 'n';

-- stmt: a partition is refused in words of its own, naming the column rather than the rule
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "j" is marked NOT NULL in parent table
-- end-expected-error
ALTER TABLE zzy8b1_pa ALTER COLUMN j DROP NOT NULL;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zzy8b1_pt_j_not_null" of relation "zzy8b1_pa"
-- end-expected-error
ALTER TABLE zzy8b1_pa DROP CONSTRAINT zzy8b1_pt_j_not_null;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_pa" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_pa (i) VALUES (1);

-- stmt: written through the partitioned table, the refusal names the partition it landed in
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_pa" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_pt (i) VALUES (1);

CREATE TABLE zzy8b1_pb (i int NOT NULL, j int, k int NOT NULL);
INSERT INTO zzy8b1_pb (i, k) VALUES (11, 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "j" in child table "zzy8b1_pb" must be marked NOT NULL
-- end-expected-error
ALTER TABLE zzy8b1_pt ATTACH PARTITION zzy8b1_pb FOR VALUES FROM (10) TO (20);

-- begin-expected
-- columns: atts
-- row: i/true/true/0,j/false/true/0,k/true/true/0
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_pb' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_inherits h JOIN pg_class cl ON cl.oid = h.inhrelid WHERE cl.relname = 'zzy8b1_pb';

DELETE FROM zzy8b1_pb;
ALTER TABLE zzy8b1_pb ALTER COLUMN j SET NOT NULL;
ALTER TABLE zzy8b1_pt ATTACH PARTITION zzy8b1_pb FOR VALUES FROM (10) TO (20);

-- stmt: what it declared is now recorded as coming from the partitioned table
-- begin-expected
-- columns: cons
-- row: zzy8b1_pb_i_not_null/false/1,zzy8b1_pb_j_not_null/false/1,zzy8b1_pb_k_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_pb' AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/true/false/1,j/true/false/1,k/true/false/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_pb' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "j" is marked NOT NULL in parent table
-- end-expected-error
ALTER TABLE zzy8b1_pb ALTER COLUMN j DROP NOT NULL;

-- stmt: k is the partition's alone, and that one it may withdraw
ALTER TABLE zzy8b1_pb ALTER COLUMN k DROP NOT NULL;

-- begin-expected
-- columns: cons
-- row: zzy8b1_pb_i_not_null/false/1,zzy8b1_pb_j_not_null/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_pb' AND c.contype = 'n';

-- stmt: withdrawn from the partitioned table, every rule it holds is its own again
ALTER TABLE zzy8b1_pt DETACH PARTITION zzy8b1_pb;

-- begin-expected
-- columns: cons
-- row: zzy8b1_pb_i_not_null/true/0,zzy8b1_pb_j_not_null/true/0
-- end-expected
SELECT string_agg(c.conname || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_pb' AND c.contype = 'n';

-- begin-expected
-- columns: atts
-- row: i/true/true/0,j/true/true/0,k/false/true/0
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_pb' AND a.attnum > 0 AND NOT a.attisdropped;

DROP TABLE zzy8b1_pb;
DROP TABLE zzy8b1_pt;

-- ============================================================================
-- A PRIMARY KEY on the child is a declaration of its own
-- ============================================================================
CREATE TABLE zzy8b1_kp (i int NOT NULL, j int NOT NULL, k int);
CREATE TABLE zzy8b1_kc (j int PRIMARY KEY) INHERITS (zzy8b1_kp);

-- begin-expected
-- columns: atts
-- row: i/true/false/1,j/true/true/1,k/false/false/1
-- end-expected
SELECT string_agg(a.attname || '/' || a.attnotnull::text || '/' || a.attislocal::text || '/' || a.attinhcount::text, ',' ORDER BY a.attnum) AS atts FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname = 'zzy8b1_kc' AND a.attnum > 0 AND NOT a.attisdropped;

-- begin-expected
-- columns: cons
-- row: zzy8b1_kc_j_not_null/n/true/1,zzy8b1_kc_pkey/p/true/0,zzy8b1_kp_i_not_null/n/false/1
-- end-expected
SELECT string_agg(c.conname || '/' || c.contype::text || '/' || c.conislocal::text || '/' || c.coninhcount::text, ',' ORDER BY c.conname) AS cons FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzy8b1_kc';

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "j" of relation "zzy8b1_kc" violates not-null constraint
-- end-expected-error
INSERT INTO zzy8b1_kc (i) VALUES (1);

DROP TABLE zzy8b1_kc;
DROP TABLE zzy8b1_kp;
