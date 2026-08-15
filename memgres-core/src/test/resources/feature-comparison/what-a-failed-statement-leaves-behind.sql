-- ============================================================================
-- What a failed statement leaves behind
--
-- Outside a transaction block a statement is a transaction of its own, so one
-- that fails leaves nothing behind -- and not only in the relation it was
-- written against. Everything the statement set going on the way counts as
-- part of it: what a rule wrote to a second relation, what a cascading foreign
-- key took out of a third, what routing put into a leaf partition. All of it
-- goes back when the statement is refused, and all of it stands when the
-- statement succeeds.
--
-- One thing does not go back: a sequence's advance. A sequence is deliberately
-- outside the transaction that read it, so the numbers a failed statement drew
-- are gone for good and the next row is numbered past them.
--
-- Every value here was read off PostgreSQL 18.
-- ============================================================================

DROP TABLE IF EXISTS zzj1jt_ft CASCADE;
DROP TABLE IF EXISTS zzj1jt_fl CASCADE;

-- ----------------------------------------------------------------------------
-- What a rule wrote to another relation goes with the statement that failed
-- ----------------------------------------------------------------------------
CREATE TABLE zzj1jt_fl (n int);
CREATE TABLE zzj1jt_ft (i int CHECK (i < 3));
CREATE RULE zzj1jt_fr AS ON INSERT TO zzj1jt_ft DO ALSO INSERT INTO zzj1jt_fl VALUES (NEW.i);

-- The first two rows are written and noted; the third fails the CHECK.
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzj1jt_ft" violates check constraint "zzj1jt_ft_i_check"
-- end-expected-error
INSERT INTO zzj1jt_ft VALUES (1),(2),(9);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzj1jt_ft;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzj1jt_fl;

-- A statement that succeeds keeps what its rule wrote.
INSERT INTO zzj1jt_ft VALUES (1),(2);

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
SELECT n FROM zzj1jt_fl ORDER BY n;

DROP TABLE zzj1jt_ft CASCADE;
DROP TABLE zzj1jt_fl CASCADE;

-- ----------------------------------------------------------------------------
-- The same when it is the rule's own write that is refused
-- ----------------------------------------------------------------------------
CREATE TABLE zzj1jt_bl (n int CHECK (n < 3));
CREATE TABLE zzj1jt_bt (i int);
CREATE RULE zzj1jt_br AS ON INSERT TO zzj1jt_bt DO ALSO INSERT INTO zzj1jt_bl VALUES (NEW.i);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzj1jt_bl" violates check constraint "zzj1jt_bl_n_check"
-- end-expected-error
INSERT INTO zzj1jt_bt VALUES (1),(9);

-- The relation the statement was written against is empty too.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzj1jt_bt;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzj1jt_bl;

INSERT INTO zzj1jt_bt VALUES (1),(2);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzj1jt_bt;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
SELECT n FROM zzj1jt_bl ORDER BY n;

DROP TABLE zzj1jt_bt CASCADE;
DROP TABLE zzj1jt_bl CASCADE;

-- ----------------------------------------------------------------------------
-- What a routed row and its rule left in a leaf partition goes too
-- ----------------------------------------------------------------------------
CREATE TABLE zzj1jt_ql (n int);
CREATE TABLE zzj1jt_qt (i int, k int CHECK (k < 100)) PARTITION BY RANGE (i);
CREATE TABLE zzj1jt_qt0 PARTITION OF zzj1jt_qt FOR VALUES FROM (0) TO (10);
CREATE TABLE zzj1jt_qt1 PARTITION OF zzj1jt_qt FOR VALUES FROM (10) TO (20);
CREATE RULE zzj1jt_qr AS ON INSERT TO zzj1jt_qt DO ALSO INSERT INTO zzj1jt_ql VALUES (NEW.i);

-- Two rows are routed into two different partitions before the third fails,
-- and the relation the refusal names is the leaf, not the partitioned table.
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzj1jt_qt0" violates check constraint "zzj1jt_qt_k_check"
-- end-expected-error
INSERT INTO zzj1jt_qt VALUES (1,1),(11,1),(2,999);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzj1jt_qt;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzj1jt_ql;

INSERT INTO zzj1jt_qt VALUES (1,1),(11,1);

-- begin-expected
-- columns: c0 | c1
-- row: 1 | 1
-- end-expected
SELECT (SELECT count(*) FROM zzj1jt_qt0) AS c0, (SELECT count(*) FROM zzj1jt_qt1) AS c1;

-- begin-expected
-- columns: n
-- row: 1
-- row: 11
-- end-expected
SELECT n FROM zzj1jt_ql ORDER BY n;

DROP TABLE zzj1jt_qt CASCADE;
DROP TABLE zzj1jt_ql CASCADE;

-- ----------------------------------------------------------------------------
-- What a cascading foreign key took out of another relation comes back
-- ----------------------------------------------------------------------------
CREATE TABLE zzj1jt_p (i int PRIMARY KEY);
CREATE TABLE zzj1jt_c (j int REFERENCES zzj1jt_p(i) ON DELETE CASCADE);
CREATE TABLE zzj1jt_n (j int REFERENCES zzj1jt_p(i) ON DELETE SET NULL);
CREATE TABLE zzj1jt_r (j int REFERENCES zzj1jt_p(i) ON DELETE RESTRICT);
INSERT INTO zzj1jt_p VALUES (1),(2);
INSERT INTO zzj1jt_c VALUES (1),(2);
INSERT INTO zzj1jt_n VALUES (1),(2);
INSERT INTO zzj1jt_r VALUES (2);

-- The delete cascades into two relations and is then refused by the third.
-- begin-expected-error
-- sqlstate: 23001
-- message-like: violates RESTRICT setting of foreign key constraint "zzj1jt_r_j_fkey" on table "zzj1jt_r"
-- end-expected-error
DELETE FROM zzj1jt_p;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzj1jt_p;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzj1jt_c;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzj1jt_n WHERE j IS NULL;

-- With the restricting row gone, the same cascade stands.
DELETE FROM zzj1jt_r;
DELETE FROM zzj1jt_p WHERE i = 1;

-- begin-expected
-- columns: j
-- row: 2
-- end-expected
SELECT j FROM zzj1jt_c ORDER BY j;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzj1jt_n WHERE j IS NULL;

DROP TABLE zzj1jt_c CASCADE;
DROP TABLE zzj1jt_r CASCADE;
DROP TABLE zzj1jt_n CASCADE;
DROP TABLE zzj1jt_p CASCADE;

-- ----------------------------------------------------------------------------
-- What does not come back: the numbers the failed statement drew
-- ----------------------------------------------------------------------------
CREATE TABLE zzj1jt_sq (i serial, k int CHECK (k < 3));
INSERT INTO zzj1jt_sq (k) VALUES (1),(2);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzj1jt_sq" violates check constraint "zzj1jt_sq_k_check"
-- end-expected-error
INSERT INTO zzj1jt_sq (k) VALUES (1),(9);

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- row: 2 | 2
-- end-expected
SELECT i, k FROM zzj1jt_sq ORDER BY i;

-- 3 and 4 were drawn by the statement that failed and are not drawn again.
INSERT INTO zzj1jt_sq (k) VALUES (2);

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- row: 2 | 2
-- row: 5 | 2
-- end-expected
SELECT i, k FROM zzj1jt_sq ORDER BY i;

DROP TABLE zzj1jt_sq CASCADE;
