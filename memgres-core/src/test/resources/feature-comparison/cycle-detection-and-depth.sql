-- ============================================================================
-- Feature Comparison: cycle detection and recursion depth
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A graph of database objects can be closed into a loop in several places:
-- ALTER TABLE ... INHERIT, ATTACH PARTITION, foreign keys that cascade into
-- each other, views defined in terms of each other, and rules or triggers that
-- write back to their own table. PostgreSQL rejects the statement that closes
-- the loop -- 42P07 for inheritance and partitioning, 42P17 for views and
-- rules -- and reports runaway recursion as 54001 rather than failing
-- internally. This file covers those refusals and the neighbouring cases that
-- must keep working: legitimate inheritance, legitimate partitioning, ordinary
-- and self-referencing cascades, view chains, rules onto another table,
-- terminating trigger chains, and PL/pgSQL recursion at ordinary depths.
-- ============================================================================

-- ============================================================================
-- 1. ALTER TABLE ... INHERIT may not close a loop
-- ============================================================================

DROP TABLE IF EXISTS cdl_a, cdl_b CASCADE;
CREATE TABLE cdl_a (x int);
CREATE TABLE cdl_b (x int);
ALTER TABLE cdl_b INHERIT cdl_a;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: circular inheritance not allowed
-- end-expected-error
ALTER TABLE cdl_a INHERIT cdl_b;

-- the refusal leaves both tables usable
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_a;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_b;

ALTER TABLE cdl_b NO INHERIT cdl_a;
DROP TABLE cdl_a, cdl_b;

-- self-inheritance is the same loop with one edge
DROP TABLE IF EXISTS cdl_self CASCADE;
CREATE TABLE cdl_self (x int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: circular inheritance not allowed
-- end-expected-error
ALTER TABLE cdl_self INHERIT cdl_self;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_self;

DROP TABLE cdl_self;

-- a loop three levels long is found the same way
DROP TABLE IF EXISTS cdl_i1, cdl_i2, cdl_i3 CASCADE;
CREATE TABLE cdl_i1 (x int);
CREATE TABLE cdl_i2 (x int);
CREATE TABLE cdl_i3 (x int);
ALTER TABLE cdl_i2 INHERIT cdl_i1;
ALTER TABLE cdl_i3 INHERIT cdl_i2;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: circular inheritance not allowed
-- end-expected-error
ALTER TABLE cdl_i1 INHERIT cdl_i3;

INSERT INTO cdl_i3 VALUES (7);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cdl_i1;

DROP TABLE cdl_i1, cdl_i2, cdl_i3;

-- ============================================================================
-- 2. Legitimate inheritance is unaffected
-- ============================================================================

DROP TABLE IF EXISTS cdl_p1, cdl_c1 CASCADE;
CREATE TABLE cdl_p1 (x int);
CREATE TABLE cdl_c1 (x int);
ALTER TABLE cdl_c1 INHERIT cdl_p1;
INSERT INTO cdl_c1 VALUES (5);
INSERT INTO cdl_p1 VALUES (6);

-- begin-expected
-- columns: x
-- row: 5
-- row: 6
-- end-expected
SELECT x FROM cdl_p1 ORDER BY x;

-- begin-expected
-- columns: x
-- row: 6
-- end-expected
SELECT x FROM ONLY cdl_p1 ORDER BY x;

DROP TABLE cdl_p1 CASCADE;

-- ============================================================================
-- 3. ATTACH PARTITION may not close a loop either
-- ============================================================================

DROP TABLE IF EXISTS cdl_pp CASCADE;
CREATE TABLE cdl_pp (i int) PARTITION BY RANGE (i);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: circular inheritance not allowed
-- end-expected-error
ALTER TABLE cdl_pp ATTACH PARTITION cdl_pp FOR VALUES FROM (1) TO (9);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_pp;

DROP TABLE cdl_pp;

-- a partitioned parent may not be attached below its own partition
DROP TABLE IF EXISTS cdl_q1, cdl_q2 CASCADE;
CREATE TABLE cdl_q1 (i int) PARTITION BY RANGE (i);
CREATE TABLE cdl_q2 PARTITION OF cdl_q1 FOR VALUES FROM (1) TO (9) PARTITION BY RANGE (i);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: circular inheritance not allowed
-- end-expected-error
ALTER TABLE cdl_q2 ATTACH PARTITION cdl_q1 FOR VALUES FROM (2) TO (3);

DROP TABLE cdl_q1 CASCADE;

-- ============================================================================
-- 4. Legitimate partitioning is unaffected, including a sub-partition
-- ============================================================================

DROP TABLE IF EXISTS cdl_r1, cdl_r2 CASCADE;
CREATE TABLE cdl_r1 (i int) PARTITION BY RANGE (i);
CREATE TABLE cdl_r2 (i int);
ALTER TABLE cdl_r1 ATTACH PARTITION cdl_r2 FOR VALUES FROM (1) TO (9);
INSERT INTO cdl_r1 VALUES (3);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cdl_r1;

DROP TABLE cdl_r1;

DROP TABLE IF EXISTS cdl_s1, cdl_s2 CASCADE;
CREATE TABLE cdl_s1 (i int) PARTITION BY RANGE (i);
CREATE TABLE cdl_s2 PARTITION OF cdl_s1 FOR VALUES FROM (1) TO (9) PARTITION BY RANGE (i);
CREATE TABLE cdl_s3 (i int);
ALTER TABLE cdl_s2 ATTACH PARTITION cdl_s3 FOR VALUES FROM (2) TO (3);
INSERT INTO cdl_s1 VALUES (2);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cdl_s3;

DROP TABLE cdl_s1 CASCADE;

-- ============================================================================
-- 5. Two tables cascading into each other on delete
-- ============================================================================

DROP TABLE IF EXISTS cdl_fa, cdl_fb CASCADE;
CREATE TABLE cdl_fa (id int PRIMARY KEY, b int);
CREATE TABLE cdl_fb (id int PRIMARY KEY, a int REFERENCES cdl_fa(id) ON DELETE CASCADE);
ALTER TABLE cdl_fa ADD FOREIGN KEY (b) REFERENCES cdl_fb(id) ON DELETE CASCADE;
INSERT INTO cdl_fa VALUES (1, NULL);
INSERT INTO cdl_fb VALUES (1, 1);
UPDATE cdl_fa SET b = 1 WHERE id = 1;
DELETE FROM cdl_fa WHERE id = 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_fa;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_fb;

DROP TABLE cdl_fa CASCADE;
DROP TABLE cdl_fb CASCADE;

-- three tables in a ring behave the same
DROP TABLE IF EXISTS cdl_c3a, cdl_c3b, cdl_c3c CASCADE;
CREATE TABLE cdl_c3a (id int PRIMARY KEY, r int);
CREATE TABLE cdl_c3b (id int PRIMARY KEY, r int REFERENCES cdl_c3a(id) ON DELETE CASCADE);
CREATE TABLE cdl_c3c (id int PRIMARY KEY, r int REFERENCES cdl_c3b(id) ON DELETE CASCADE);
ALTER TABLE cdl_c3a ADD FOREIGN KEY (r) REFERENCES cdl_c3c(id) ON DELETE CASCADE;
INSERT INTO cdl_c3a VALUES (1, NULL);
INSERT INTO cdl_c3b VALUES (1, 1);
INSERT INTO cdl_c3c VALUES (1, 1);
UPDATE cdl_c3a SET r = 1 WHERE id = 1;
DELETE FROM cdl_c3a WHERE id = 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_c3b;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_c3c;

DROP TABLE cdl_c3a CASCADE;
DROP TABLE cdl_c3b CASCADE;
DROP TABLE cdl_c3c CASCADE;

-- ============================================================================
-- 6. A ring of ON UPDATE CASCADE keys propagates once, not forever
-- ============================================================================

DROP TABLE IF EXISTS cdl_ua, cdl_ub CASCADE;
CREATE TABLE cdl_ua (id int PRIMARY KEY, r int);
CREATE TABLE cdl_ub (id int PRIMARY KEY, r int REFERENCES cdl_ua(id) ON UPDATE CASCADE);
ALTER TABLE cdl_ua ADD FOREIGN KEY (r) REFERENCES cdl_ub(id) ON UPDATE CASCADE;
INSERT INTO cdl_ua VALUES (1, NULL);
INSERT INTO cdl_ub VALUES (1, 1);
UPDATE cdl_ua SET r = 1 WHERE id = 1;
UPDATE cdl_ua SET id = 2 WHERE id = 1;

-- begin-expected
-- columns: id|r
-- row: 2|1
-- end-expected
SELECT id, r FROM cdl_ua;

-- begin-expected
-- columns: id|r
-- row: 1|2
-- end-expected
SELECT id, r FROM cdl_ub;

DROP TABLE cdl_ua CASCADE;
DROP TABLE cdl_ub CASCADE;

-- ============================================================================
-- 7. A row that references its own key deletes cleanly
-- ============================================================================

DROP TABLE IF EXISTS cdl_t CASCADE;
CREATE TABLE cdl_t (a int PRIMARY KEY, b int, FOREIGN KEY (b) REFERENCES cdl_t(a) ON DELETE CASCADE);
INSERT INTO cdl_t VALUES (0, 0);
DELETE FROM cdl_t WHERE a = 0;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_t;

DROP TABLE cdl_t;

-- a self-referencing chain cascades the whole way down
DROP TABLE IF EXISTS cdl_tc CASCADE;
CREATE TABLE cdl_tc (a int PRIMARY KEY, b int REFERENCES cdl_tc(a) ON DELETE CASCADE);
INSERT INTO cdl_tc VALUES (1, NULL);
INSERT INTO cdl_tc VALUES (2, 1);
INSERT INTO cdl_tc VALUES (3, 2);
DELETE FROM cdl_tc WHERE a = 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_tc;

DROP TABLE cdl_tc;

-- ============================================================================
-- 8. Ordinary cascades and RESTRICT are unaffected
-- ============================================================================

DROP TABLE IF EXISTS cdl_ga, cdl_gb CASCADE;
CREATE TABLE cdl_ga (id int PRIMARY KEY);
CREATE TABLE cdl_gb (id int PRIMARY KEY, a int REFERENCES cdl_ga(id) ON DELETE CASCADE);
INSERT INTO cdl_ga VALUES (1), (2);
INSERT INTO cdl_gb VALUES (10, 1), (20, 2);
DELETE FROM cdl_ga WHERE id = 1;

-- begin-expected
-- columns: id
-- row: 20
-- end-expected
SELECT id FROM cdl_gb ORDER BY id;

DROP TABLE cdl_gb, cdl_ga;

DROP TABLE IF EXISTS cdl_ha, cdl_hb CASCADE;
CREATE TABLE cdl_ha (id int PRIMARY KEY);
CREATE TABLE cdl_hb (id int PRIMARY KEY, a int REFERENCES cdl_ha(id));
INSERT INTO cdl_ha VALUES (1);
INSERT INTO cdl_hb VALUES (10, 1);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
DELETE FROM cdl_ha WHERE id = 1;

DROP TABLE cdl_hb, cdl_ha;

-- ============================================================================
-- 9. Views defined in terms of each other
-- ============================================================================

DROP VIEW IF EXISTS cdl_v2, cdl_v1 CASCADE;
DROP TABLE IF EXISTS cdl_vt CASCADE;
CREATE TABLE cdl_vt (i int);
CREATE VIEW cdl_v1 AS SELECT i FROM cdl_vt;
CREATE VIEW cdl_v2 AS SELECT i FROM cdl_v1;
CREATE OR REPLACE VIEW cdl_v1 AS SELECT i FROM cdl_v2;

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: infinite recursion detected in rules for relation "cdl_v1"
-- end-expected-error
SELECT count(*) FROM cdl_v1;

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: infinite recursion detected in rules for relation "cdl_v2"
-- end-expected-error
SELECT count(*) FROM cdl_v2;

DROP VIEW cdl_v1 CASCADE;
DROP TABLE cdl_vt CASCADE;

-- a view replaced by one selecting from itself
DROP VIEW IF EXISTS cdl_sv CASCADE;
DROP TABLE IF EXISTS cdl_svt CASCADE;
CREATE TABLE cdl_svt (i int);
CREATE VIEW cdl_sv AS SELECT i FROM cdl_svt;
CREATE OR REPLACE VIEW cdl_sv AS SELECT i FROM cdl_sv;

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: infinite recursion detected in rules for relation "cdl_sv"
-- end-expected-error
SELECT count(*) FROM cdl_sv;

DROP VIEW cdl_sv;
DROP TABLE cdl_svt;

-- ============================================================================
-- 10. Ordinary view chains still resolve
-- ============================================================================

DROP VIEW IF EXISTS cdl_w2, cdl_w1 CASCADE;
DROP TABLE IF EXISTS cdl_wt CASCADE;
CREATE TABLE cdl_wt (i int);
INSERT INTO cdl_wt VALUES (1), (2);
CREATE VIEW cdl_w1 AS SELECT i FROM cdl_wt;
CREATE VIEW cdl_w2 AS SELECT i FROM cdl_w1 WHERE i > 1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cdl_w2;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT i FROM cdl_w1 ORDER BY i;

DROP VIEW cdl_w2;
DROP VIEW cdl_w1;
DROP TABLE cdl_wt;

-- ============================================================================
-- 11. A trigger that inserts into its own table
-- ============================================================================

DROP TABLE IF EXISTS cdl_trg CASCADE;
DROP FUNCTION IF EXISTS cdl_trg_f() CASCADE;
CREATE TABLE cdl_trg (i int);
CREATE FUNCTION cdl_trg_f() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO cdl_trg VALUES (NEW.i + 1); RETURN NEW; END;
$$;
CREATE TRIGGER cdl_trg_t AFTER INSERT ON cdl_trg FOR EACH ROW EXECUTE FUNCTION cdl_trg_f();

-- begin-expected-error
-- sqlstate: 54001
-- message-like: stack depth limit exceeded
-- end-expected-error
INSERT INTO cdl_trg VALUES (1);

-- the failed statement leaves nothing behind
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_trg;

DROP TABLE cdl_trg CASCADE;

-- ============================================================================
-- 12. Trigger chains that terminate still run to completion
-- ============================================================================

DROP TABLE IF EXISTS cdl_src, cdl_log CASCADE;
DROP FUNCTION IF EXISTS cdl_log_f() CASCADE;
CREATE TABLE cdl_src (i int);
CREATE TABLE cdl_log (i int);
CREATE FUNCTION cdl_log_f() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO cdl_log VALUES (NEW.i); RETURN NEW; END;
$$;
CREATE TRIGGER cdl_log_t AFTER INSERT ON cdl_src FOR EACH ROW EXECUTE FUNCTION cdl_log_f();
INSERT INTO cdl_src VALUES (1), (2);

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT i FROM cdl_log ORDER BY i;

DROP TABLE cdl_src CASCADE;
DROP TABLE cdl_log CASCADE;

DROP TABLE IF EXISTS cdl_dp CASCADE;
DROP FUNCTION IF EXISTS cdl_dp_f() CASCADE;
CREATE TABLE cdl_dp (i int);
CREATE FUNCTION cdl_dp_f() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.i < 50 THEN INSERT INTO cdl_dp VALUES (NEW.i + 1); END IF; RETURN NEW; END;
$$;
CREATE TRIGGER cdl_dp_t AFTER INSERT ON cdl_dp FOR EACH ROW EXECUTE FUNCTION cdl_dp_f();
INSERT INTO cdl_dp VALUES (1);

-- begin-expected
-- columns: count
-- row: 50
-- end-expected
SELECT count(*) FROM cdl_dp;

DROP TABLE cdl_dp CASCADE;

-- ============================================================================
-- 13. Rules that rewrite onto their own table
-- ============================================================================

DROP TABLE IF EXISTS cdl_rt CASCADE;
CREATE TABLE cdl_rt (i int);
CREATE RULE cdl_rt_r AS ON INSERT TO cdl_rt DO ALSO INSERT INTO cdl_rt VALUES (NEW.i + 1);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: infinite recursion detected in rules for relation "cdl_rt"
-- end-expected-error
INSERT INTO cdl_rt VALUES (1);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_rt;

DROP TABLE cdl_rt CASCADE;

DROP TABLE IF EXISTS cdl_it CASCADE;
CREATE TABLE cdl_it (i int);
CREATE RULE cdl_it_r AS ON INSERT TO cdl_it DO INSTEAD INSERT INTO cdl_it VALUES (NEW.i);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: infinite recursion detected in rules for relation "cdl_it"
-- end-expected-error
INSERT INTO cdl_it VALUES (1);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cdl_it;

DROP TABLE cdl_it CASCADE;

-- ============================================================================
-- 14. A rule onto another table is unaffected
-- ============================================================================

DROP TABLE IF EXISTS cdl_ra, cdl_rb CASCADE;
CREATE TABLE cdl_ra (i int);
CREATE TABLE cdl_rb (i int);
CREATE RULE cdl_ra_r AS ON INSERT TO cdl_ra DO ALSO INSERT INTO cdl_rb VALUES (NEW.i);
INSERT INTO cdl_ra VALUES (1);

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i FROM cdl_rb ORDER BY i;

DROP TABLE cdl_ra, cdl_rb CASCADE;

-- ============================================================================
-- 15. PL/pgSQL recursion: ordinary depths work, runaway recursion is 54001
-- ============================================================================

DROP FUNCTION IF EXISTS cdl_f(int);
CREATE FUNCTION cdl_f(n int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN IF n <= 0 THEN RETURN 0; END IF; RETURN 1 + cdl_f(n - 1); END;
$$;

-- begin-expected
-- columns: cdl_f
-- row: 0
-- end-expected
SELECT cdl_f(0);

-- begin-expected
-- columns: cdl_f
-- row: 100
-- end-expected
SELECT cdl_f(100);

-- begin-expected
-- columns: cdl_f
-- row: 500
-- end-expected
SELECT cdl_f(500);

-- begin-expected-error
-- sqlstate: 54001
-- message-like: stack depth limit exceeded
-- end-expected-error
SELECT cdl_f(100000);

DROP FUNCTION cdl_f(int);

-- mutual recursion is bounded the same way
DROP FUNCTION IF EXISTS cdl_m1(int);
DROP FUNCTION IF EXISTS cdl_m2(int);
CREATE FUNCTION cdl_m2(n int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN RETURN n; END;
$$;
CREATE FUNCTION cdl_m1(n int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN IF n <= 0 THEN RETURN 0; END IF; RETURN 1 + cdl_m2(n - 1); END;
$$;
CREATE OR REPLACE FUNCTION cdl_m2(n int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN IF n <= 0 THEN RETURN 0; END IF; RETURN 1 + cdl_m1(n - 1); END;
$$;

-- begin-expected
-- columns: cdl_m1
-- row: 100
-- end-expected
SELECT cdl_m1(100);

-- begin-expected-error
-- sqlstate: 54001
-- message-like: stack depth limit exceeded
-- end-expected-error
SELECT cdl_m1(100000);

DROP FUNCTION cdl_m1(int);
DROP FUNCTION cdl_m2(int);
