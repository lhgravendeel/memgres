-- ============================================================================
-- MERGE sees the rows of a partitioned target
-- ============================================================================
CREATE TABLE wmg_ff (k int, v text) PARTITION BY RANGE (k);
CREATE TABLE wmg_ff_1 PARTITION OF wmg_ff FOR VALUES FROM (1) TO (10);
CREATE TABLE wmg_ff_2 PARTITION OF wmg_ff FOR VALUES FROM (10) TO (20);
CREATE TABLE wmg_ffs (k int, v text);
INSERT INTO wmg_ff VALUES (5,'old');
INSERT INTO wmg_ffs VALUES (5,'new');

MERGE INTO wmg_ff t USING wmg_ffs u ON t.k = u.k WHEN MATCHED THEN UPDATE SET v = u.v;

-- begin-expected
-- columns: k, v
-- row: 5, new
-- end-expected
SELECT k, v FROM wmg_ff ORDER BY k;

MERGE INTO wmg_ff t USING wmg_ffs u ON t.k = u.k
  WHEN MATCHED THEN UPDATE SET v = 'A'
  WHEN NOT MATCHED THEN INSERT (k,v) VALUES (u.k,'a');

-- begin-expected
-- columns: k, v
-- row: 5, A
-- end-expected
SELECT k, v FROM wmg_ff ORDER BY k, v;

MERGE INTO wmg_ff t USING wmg_ffs u ON t.k = u.k WHEN MATCHED THEN UPDATE SET k = 15;

-- begin-expected
-- columns: c1, c2
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*) FROM wmg_ff_1) AS c1, (SELECT count(*) FROM wmg_ff_2) AS c2;

MERGE INTO wmg_ff t USING wmg_ffs u ON t.k = u.k WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wmg_ff;

INSERT INTO wmg_ff VALUES (5,'x');
INSERT INTO wmg_ffs VALUES (5,'second');

-- begin-expected-error
-- sqlstate: 21000
-- message-like: MERGE command cannot affect row a second time
-- end-expected-error
MERGE INTO wmg_ff t USING wmg_ffs u ON t.k = u.k WHEN MATCHED THEN UPDATE SET v = u.v;

DROP TABLE wmg_ff CASCADE;
DROP TABLE wmg_ffs;

-- ============================================================================
-- A failed MERGE leaves nothing behind in a leaf partition
-- ============================================================================
CREATE TABLE wmg_pr (k int, v int, CHECK (v < 100)) PARTITION BY RANGE (k);
CREATE TABLE wmg_pr_1 PARTITION OF wmg_pr FOR VALUES FROM (1) TO (10);
CREATE TABLE wmg_prs (k int, v int);
INSERT INTO wmg_prs VALUES (1,1),(2,500);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint
-- end-expected-error
MERGE INTO wmg_pr t USING wmg_prs u ON t.k = u.k
  WHEN NOT MATCHED THEN INSERT (k,v) VALUES (u.k, u.v);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wmg_pr;

DROP TABLE wmg_pr CASCADE;
DROP TABLE wmg_prs;

-- ============================================================================
-- MERGE in a read-only transaction
-- ============================================================================
CREATE TABLE wmg_ro (id int, v int);
INSERT INTO wmg_ro VALUES (1,1);

BEGIN;
SET TRANSACTION READ ONLY;

-- begin-expected-error
-- sqlstate: 25006
-- message-like: cannot execute MERGE in a read-only transaction
-- end-expected-error
MERGE INTO wmg_ro t USING (VALUES (2)) s(id) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, 5);

ROLLBACK;

BEGIN;
SET TRANSACTION READ ONLY;

-- begin-expected-error
-- sqlstate: 25006
-- message-like: cannot execute MERGE in a read-only transaction
-- end-expected-error
MERGE INTO wmg_ro t USING (VALUES (99)) s(id) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = 7;

ROLLBACK;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM wmg_ro;

DROP TABLE wmg_ro;

-- ============================================================================
-- MERGE's INSERT arm is held to an INSERT's arity rules
-- ============================================================================
CREATE TABLE wmg_a1 (id int, v int, w int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: INSERT has more expressions than target columns
-- end-expected-error
MERGE INTO wmg_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id, s.v);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: INSERT has more expressions than target columns
-- end-expected-error
MERGE INTO wmg_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v, 5, 6);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: INSERT has more target columns than expressions
-- end-expected-error
MERGE INTO wmg_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id);

MERGE INTO wmg_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v);

-- begin-expected
-- columns: id, v, w
-- row: 1, 2, NULL
-- end-expected
SELECT id, v, w FROM wmg_a1;

DROP TABLE wmg_a1;

-- ============================================================================
-- MERGE and columns the system computes, and domains
-- ============================================================================
CREATE TABLE wmg_gt (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a*2) STORED);
INSERT INTO wmg_gt (id,a) VALUES (1,1);

-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
MERGE INTO wmg_gt t USING (SELECT 1 AS id) s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET g = 99;

-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
MERGE INTO wmg_gt t USING (VALUES (99)) s(id) ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET g = 42;

MERGE INTO wmg_gt t USING (VALUES (2,3)) s(id,a) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, a, g) VALUES (s.id, s.a, DEFAULT);

-- begin-expected
-- columns: id, a, g
-- row: 1, 1, 2
-- row: 2, 3, 6
-- end-expected
SELECT id, a, g FROM wmg_gt ORDER BY id;

CREATE TABLE wmg_id1 (id int GENERATED ALWAYS AS IDENTITY, v int);

-- begin-expected-error
-- sqlstate: 428C9
-- message-like: cannot insert a non-DEFAULT value into column "id"
-- end-expected-error
MERGE INTO wmg_id1 t USING (VALUES (5)) s(v) ON t.v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (9, s.v);

MERGE INTO wmg_id1 t USING (VALUES (5)) s(v) ON t.v = s.v
  WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v);

-- begin-expected
-- columns: id, v
-- row: 1, 5
-- end-expected
SELECT id, v FROM wmg_id1 ORDER BY id;

CREATE TABLE wmg_d1 (id int DEFAULT 7, v int);
MERGE INTO wmg_d1 t USING (VALUES (99)) s(x) ON t.id = s.x
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (DEFAULT, 5);

-- begin-expected
-- columns: id, v
-- row: 7, 5
-- end-expected
SELECT id, v FROM wmg_d1 ORDER BY id, v;

CREATE DOMAIN wmg_dom AS int CHECK (VALUE < 100);
CREATE TABLE wmg_dt (id int PRIMARY KEY, v wmg_dom);
INSERT INTO wmg_dt VALUES (1,1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain wmg_dom violates check constraint "wmg_dom_check"
-- end-expected-error
MERGE INTO wmg_dt t USING (VALUES (1)) s(id) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = 500;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain wmg_dom violates check constraint "wmg_dom_check"
-- end-expected-error
MERGE INTO wmg_dt t USING (VALUES (2)) s(id) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, 500);

-- begin-expected
-- columns: id, v
-- row: 1, 1
-- end-expected
SELECT id, v FROM wmg_dt ORDER BY id;

DROP TABLE wmg_gt;
DROP TABLE wmg_id1;
DROP TABLE wmg_d1;
DROP TABLE wmg_dt;
DROP DOMAIN wmg_dom;

-- ============================================================================
-- MERGE through a view
-- ============================================================================
CREATE TABLE wmg_ga (i int PRIMARY KEY, v int);
INSERT INTO wmg_ga VALUES (1,1);
CREATE VIEW wmg_gav AS SELECT i, v FROM wmg_ga WHERE v < 10 WITH CHECK OPTION;

-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "wmg_gav"
-- end-expected-error
MERGE INTO wmg_gav t USING (VALUES (1,90)) s(i,v) ON t.i = s.i
  WHEN MATCHED THEN UPDATE SET v = s.v;

-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "wmg_gav"
-- end-expected-error
MERGE INTO wmg_gav t USING (VALUES (7,90)) s(i,v) ON t.i = s.i
  WHEN NOT MATCHED THEN INSERT (i,v) VALUES (s.i, s.v);

-- begin-expected
-- columns: i, v
-- row: 1, 1
-- end-expected
SELECT i, v FROM wmg_ga ORDER BY i;

CREATE TABLE wmg_vb (a int PRIMARY KEY, b int, c int);
INSERT INTO wmg_vb VALUES (1,20,30);
CREATE VIEW wmg_vv AS SELECT c AS x, a AS y, b AS z FROM wmg_vb;

MERGE INTO wmg_vv t USING (VALUES (1)) s(k) ON t.y = s.k WHEN MATCHED THEN UPDATE SET x = 77;

-- begin-expected
-- columns: a, b, c
-- row: 1, 20, 77
-- end-expected
SELECT a, b, c FROM wmg_vb ORDER BY a;

DROP VIEW wmg_gav;
DROP VIEW wmg_vv;
DROP TABLE wmg_ga;
DROP TABLE wmg_vb;

-- ============================================================================
-- MERGE is refused on a relation that carries a rule
-- ============================================================================
CREATE TABLE wmg_r6 (i int primary key, v text);
CREATE TABLE wmg_r6log (m text);
CREATE RULE wmg_r6_h AS ON INSERT TO wmg_r6 DO ALSO INSERT INTO wmg_r6log VALUES ('i');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot execute MERGE on relation "wmg_r6"
-- end-expected-error
MERGE INTO wmg_r6 t USING (SELECT 1 AS i) s ON t.i = s.i WHEN NOT MATCHED THEN DO NOTHING;

CREATE RULE wmg_r6_u AS ON UPDATE TO wmg_r6 DO ALSO INSERT INTO wmg_r6log VALUES ('u');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot execute MERGE on relation "wmg_r6"
-- end-expected-error
MERGE INTO wmg_r6 t USING (SELECT 1 AS i) s ON t.i = s.i WHEN MATCHED THEN UPDATE SET v = 'z';

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wmg_r6log;

CREATE TABLE wmg_rb (i int primary key, v text);
CREATE VIEW wmg_rv AS SELECT i, v FROM wmg_rb;
CREATE RULE wmg_rv_i AS ON INSERT TO wmg_rv DO INSTEAD INSERT INTO wmg_rb VALUES (NEW.i, NEW.v);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot execute MERGE on relation "wmg_rv"
-- end-expected-error
MERGE INTO wmg_rv t USING (VALUES (1,'a')) s(i,v) ON t.i = s.i
  WHEN NOT MATCHED THEN INSERT (i,v) VALUES (s.i, s.v);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wmg_rb;

DROP VIEW wmg_rv;
DROP TABLE wmg_rb;
DROP TABLE wmg_r6 CASCADE;
DROP TABLE wmg_r6log;

-- ============================================================================
-- MERGE resolves every name before it scans, and consumes its whole statement
-- ============================================================================
CREATE TABLE wmg_e1 (id int, v int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "wmg_e1" does not exist
-- end-expected-error
MERGE INTO wmg_e1 t USING (SELECT 1 AS id WHERE false) s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (nosuch) VALUES (1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column t.nosuch does not exist
-- end-expected-error
MERGE INTO wmg_e1 t USING (SELECT 1 AS id WHERE false) s ON t.nosuch = s.id
  WHEN MATCHED THEN UPDATE SET v = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column t.nosuch does not exist
-- end-expected-error
MERGE INTO wmg_e1 t USING (SELECT 1 AS id) s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = t.nosuch;

INSERT INTO wmg_e1 VALUES (1,1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot assign to system column "ctid"
-- end-expected-error
MERGE INTO wmg_e1 t USING (VALUES (1)) s(id) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET ctid = '(0,1)';

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "t" of relation "wmg_e1" does not exist
-- end-expected-error
MERGE INTO wmg_e1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (t.id, t.v) VALUES (s.id, s.v);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "WHERE"
-- end-expected-error
MERGE INTO wmg_e1 t USING (VALUES (1)) s(id) ON t.id = s.id
  WHEN MATCHED THEN DELETE WHERE 1 = 0;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "wmg_garbage"
-- end-expected-error
MERGE INTO wmg_e1 t USING (VALUES (1)) s(id) ON t.id = s.id
  WHEN MATCHED THEN DELETE wmg_garbage;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM wmg_e1;

DROP TABLE wmg_e1;

-- ============================================================================
-- MERGE RETURNING reads over the join: the source's columns come first
-- ============================================================================
CREATE TABLE wmg_o1 (ta int, tb int);
CREATE TABLE wmg_o2 (sa int, sb int, sc int);
INSERT INTO wmg_o1 VALUES (11,12);
INSERT INTO wmg_o2 VALUES (11,22,23);

-- begin-expected
-- columns: sa, sb, sc, ta, tb
-- row: 11, 22, 23, 11, 99
-- end-expected
MERGE INTO wmg_o1 t USING wmg_o2 s ON t.ta = s.sa
  WHEN MATCHED THEN UPDATE SET tb = 99 RETURNING *;

-- begin-expected
-- columns: sa, sb, sc
-- row: 11, 22, 23
-- end-expected
MERGE INTO wmg_o1 t USING wmg_o2 s ON t.ta = s.sa
  WHEN MATCHED THEN UPDATE SET tb = 97 RETURNING s.*;

-- begin-expected
-- columns: ta, tb
-- row: 11, 95
-- end-expected
MERGE INTO wmg_o1 t USING wmg_o2 s ON t.ta = s.sa
  WHEN MATCHED THEN UPDATE SET tb = 95 RETURNING t.*;

-- begin-expected
-- columns: sa, sb, ta, tb
-- row: 11, 55, 11, 96
-- end-expected
MERGE INTO wmg_o1 t USING (VALUES (11,55)) s(sa,sb) ON t.ta = s.sa
  WHEN MATCHED THEN UPDATE SET tb = 96 RETURNING *;

-- begin-expected
-- columns: sa, sb, sc, ta, tb
-- row: NULL, NULL, NULL, 11, 93
-- end-expected
MERGE INTO wmg_o1 t USING wmg_o2 s ON t.ta = 999
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET tb = 93 RETURNING *;

DROP TABLE wmg_o1;
DROP TABLE wmg_o2;

-- ============================================================================
-- MERGE fires the statement triggers of every action its arms could perform
-- ============================================================================
CREATE TABLE wmg_tl (m text);
CREATE TABLE wmg_tt (id int PRIMARY KEY, v int);
INSERT INTO wmg_tt VALUES (1,1);
CREATE FUNCTION wmg_tf() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO wmg_tl VALUES (TG_WHEN || TG_OP || TG_LEVEL); RETURN NULL; END;
$$;
CREATE TRIGGER wmg_su BEFORE UPDATE ON wmg_tt FOR EACH STATEMENT EXECUTE FUNCTION wmg_tf();
CREATE TRIGGER wmg_su2 AFTER UPDATE ON wmg_tt FOR EACH STATEMENT EXECUTE FUNCTION wmg_tf();
CREATE TRIGGER wmg_si BEFORE INSERT ON wmg_tt FOR EACH STATEMENT EXECUTE FUNCTION wmg_tf();
CREATE TRIGGER wmg_si2 AFTER INSERT ON wmg_tt FOR EACH STATEMENT EXECUTE FUNCTION wmg_tf();

MERGE INTO wmg_tt t USING (VALUES (1,3),(5,5)) s(id,v) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);

-- begin-expected
-- columns: m
-- row: AFTERINSERTSTATEMENT
-- row: AFTERUPDATESTATEMENT
-- row: BEFOREINSERTSTATEMENT
-- row: BEFOREUPDATESTATEMENT
-- end-expected
SELECT m FROM wmg_tl ORDER BY m;

DROP TABLE wmg_tt CASCADE;
DROP TABLE wmg_tl;
DROP FUNCTION wmg_tf();