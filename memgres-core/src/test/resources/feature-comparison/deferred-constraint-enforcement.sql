-- ============================================================================
-- Feature Comparison: deferred constraint enforcement
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Postponing a check to COMMIT changes when it runs, never whether it runs. A
-- transaction that wrote a duplicate under a DEFERRABLE UNIQUE and then rolled
-- back used to leave the constraint unenforced for good, because the unique
-- index kept one row per key: the transient duplicate overwrote the committed
-- row's entry, and undoing the duplicate removed the key outright.
--
-- What is pinned here: that an aborted transaction leaves the key held; that
-- the referenced side of a NO ACTION foreign key waits for COMMIT while
-- RESTRICT still refuses the statement; that a rolled back subtransaction
-- takes its postponed checks with it; and that every ordinary shape around
-- them keeps working. The cross-connection part lives in the unit test.
-- ============================================================================

-- ============================================================================
-- 1. An aborted transaction leaves the constraint enforced
-- ============================================================================
DROP TABLE IF EXISTS dce_u CASCADE;
CREATE TABLE dce_u (i int PRIMARY KEY, j int,
                    CONSTRAINT dce_uu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_u VALUES (1,1);

BEGIN;
INSERT INTO dce_u VALUES (2,1);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_uu"
-- end-expected-error
INSERT INTO dce_u VALUES (3,1);

-- begin-expected
-- columns: j
-- row: 1
-- end-expected
SELECT j::text AS j FROM dce_u ORDER BY i;

-- the same after a COMMIT that failed on the postponed check
BEGIN;
INSERT INTO dce_u VALUES (4,1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_uu"
-- end-expected-error
COMMIT;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_uu"
-- end-expected-error
INSERT INTO dce_u VALUES (5,1);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_u;

-- and after an UPDATE that was rolled back
DROP TABLE IF EXISTS dce_up CASCADE;
CREATE TABLE dce_up (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_upu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_up VALUES (1,1),(2,2);
BEGIN;
UPDATE dce_up SET j = 1 WHERE i = 2;
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_upu"
-- end-expected-error
UPDATE dce_up SET j = 1 WHERE i = 2;

-- begin-expected
-- columns: j
-- row: 1
-- row: 2
-- end-expected
SELECT j::text AS j FROM dce_up ORDER BY i;

-- ============================================================================
-- 2. Primary keys, multi-column keys and repeated cycles behave the same
-- ============================================================================
DROP TABLE IF EXISTS dce_pk CASCADE;
CREATE TABLE dce_pk (i int, CONSTRAINT dce_pkp PRIMARY KEY (i) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_pk VALUES (1);
BEGIN;
INSERT INTO dce_pk VALUES (1);
ROLLBACK;
BEGIN;
INSERT INTO dce_pk VALUES (1);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_pkp"
-- end-expected-error
INSERT INTO dce_pk VALUES (1);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_pk;

DROP TABLE IF EXISTS dce_mc CASCADE;
CREATE TABLE dce_mc (a int, b int,
                     CONSTRAINT dce_mcp PRIMARY KEY (a,b) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_mc VALUES (1,1);
BEGIN;
INSERT INTO dce_mc VALUES (1,1);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_mcp"
-- end-expected-error
INSERT INTO dce_mc VALUES (1,1);

-- a different key is still free
INSERT INTO dce_mc VALUES (1,2);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM dce_mc;

-- ============================================================================
-- 3. Two constraints on one table, and the same shape on two tables
-- ============================================================================
DROP TABLE IF EXISTS dce_two CASCADE;
CREATE TABLE dce_two (i int PRIMARY KEY, j int, k int,
                      CONSTRAINT dce_twa UNIQUE (j) DEFERRABLE INITIALLY DEFERRED,
                      CONSTRAINT dce_twb UNIQUE (k) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_two VALUES (1,1,1);
BEGIN;
INSERT INTO dce_two VALUES (2,1,2);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_twa"
-- end-expected-error
INSERT INTO dce_two VALUES (3,1,3);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_twb"
-- end-expected-error
INSERT INTO dce_two VALUES (4,4,1);

DROP TABLE IF EXISTS dce_ta CASCADE;
DROP TABLE IF EXISTS dce_tb CASCADE;
CREATE TABLE dce_ta (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_tau UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE dce_tb (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_tbu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_ta VALUES (1,1);
INSERT INTO dce_tb VALUES (1,1);
BEGIN;
INSERT INTO dce_ta VALUES (2,1);
INSERT INTO dce_tb VALUES (2,1);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_tau"
-- end-expected-error
INSERT INTO dce_ta VALUES (3,1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_tbu"
-- end-expected-error
INSERT INTO dce_tb VALUES (3,1);

-- ============================================================================
-- 4. DEFERRABLE INITIALLY IMMEDIATE, deferred by hand
-- ============================================================================
DROP TABLE IF EXISTS dce_ii CASCADE;
CREATE TABLE dce_ii (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_iiu UNIQUE (j) DEFERRABLE INITIALLY IMMEDIATE);
INSERT INTO dce_ii VALUES (1,1);

-- without SET CONSTRAINTS it fires at the statement
BEGIN;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_iiu"
-- end-expected-error
INSERT INTO dce_ii VALUES (2,1);
ROLLBACK;

BEGIN;
SET CONSTRAINTS dce_iiu DEFERRED;
INSERT INTO dce_ii VALUES (3,1);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_iiu"
-- end-expected-error
INSERT INTO dce_ii VALUES (4,1);

-- ============================================================================
-- 5. A rolled back subtransaction takes its postponed checks with it
-- ============================================================================
DROP TABLE IF EXISTS dce_sp CASCADE;
CREATE TABLE dce_sp (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_spu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_sp VALUES (1,1);
BEGIN;
SAVEPOINT sp1;
INSERT INTO dce_sp VALUES (2,1);
ROLLBACK TO SAVEPOINT sp1;
INSERT INTO dce_sp VALUES (3,7);
COMMIT;

-- begin-expected
-- columns: j
-- row: 1
-- row: 7
-- end-expected
SELECT j::text AS j FROM dce_sp ORDER BY i;

-- a duplicate written before the savepoint survives the rollback
BEGIN;
INSERT INTO dce_sp VALUES (4,1);
SAVEPOINT sp2;
INSERT INTO dce_sp VALUES (5,9);
ROLLBACK TO SAVEPOINT sp2;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_spu"
-- end-expected-error
COMMIT;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM dce_sp;

-- the same for a foreign key
DROP TABLE IF EXISTS dce_fkc CASCADE;
DROP TABLE IF EXISTS dce_fkp CASCADE;
CREATE TABLE dce_fkp (i int PRIMARY KEY);
CREATE TABLE dce_fkc (i int PRIMARY KEY, p int,
                      CONSTRAINT dce_fk FOREIGN KEY (p) REFERENCES dce_fkp(i)
                      DEFERRABLE INITIALLY DEFERRED);
BEGIN;
SAVEPOINT sp1;
INSERT INTO dce_fkc VALUES (1,99);
ROLLBACK TO SAVEPOINT sp1;
COMMIT;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dce_fkc;

-- released rather than rolled back, the same insert still fails at COMMIT
BEGIN;
SAVEPOINT sp2;
INSERT INTO dce_fkc VALUES (1,99);
RELEASE SAVEPOINT sp2;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint "dce_fk"
-- end-expected-error
COMMIT;

-- ============================================================================
-- 6. The referenced side of a deferred NO ACTION foreign key waits for COMMIT
-- ============================================================================
DROP TABLE IF EXISTS dce_rc CASCADE;
DROP TABLE IF EXISTS dce_rp CASCADE;
CREATE TABLE dce_rp (i int PRIMARY KEY);
CREATE TABLE dce_rc (i int PRIMARY KEY, p int,
                     CONSTRAINT dce_rf FOREIGN KEY (p) REFERENCES dce_rp(i)
                     DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_rp VALUES (1);
INSERT INTO dce_rc VALUES (1,1);

BEGIN;
DELETE FROM dce_rp WHERE i = 1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dce_rp;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "dce_rp" violates foreign key constraint "dce_rf"
-- end-expected-error
COMMIT;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_rp;

-- putting the key back inside the transaction makes it pass
BEGIN;
DELETE FROM dce_rp WHERE i = 1;
INSERT INTO dce_rp VALUES (1);
COMMIT;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_rp;

-- so does moving the child out of the way
BEGIN;
DELETE FROM dce_rp WHERE i = 1;
DELETE FROM dce_rc WHERE p = 1;
COMMIT;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dce_rp;

-- rolling the delete back to a savepoint leaves nothing pending
INSERT INTO dce_rp VALUES (1);
INSERT INTO dce_rc VALUES (1,1);
BEGIN;
SAVEPOINT sp1;
DELETE FROM dce_rp WHERE i = 1;
ROLLBACK TO SAVEPOINT sp1;
COMMIT;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_rp;

-- ============================================================================
-- 7. A referenced key changed and put back again
-- ============================================================================
DROP TABLE IF EXISTS dce_kc CASCADE;
DROP TABLE IF EXISTS dce_kp CASCADE;
CREATE TABLE dce_kp (i int PRIMARY KEY);
CREATE TABLE dce_kc (i int PRIMARY KEY, p int,
                     CONSTRAINT dce_kf FOREIGN KEY (p) REFERENCES dce_kp(i)
                     DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_kp VALUES (1);
INSERT INTO dce_kc VALUES (1,1);
BEGIN;
UPDATE dce_kp SET i = 2 WHERE i = 1;
UPDATE dce_kp SET i = 1 WHERE i = 2;
COMMIT;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i::text AS i FROM dce_kp ORDER BY i;

BEGIN;
UPDATE dce_kp SET i = 3 WHERE i = 1;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "dce_kp" violates foreign key constraint "dce_kf"
-- end-expected-error
COMMIT;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i::text AS i FROM dce_kp ORDER BY i;

-- ============================================================================
-- 8. SET CONSTRAINTS raises a pending check where it stands
-- ============================================================================
DROP TABLE IF EXISTS dce_ic CASCADE;
DROP TABLE IF EXISTS dce_ip CASCADE;
CREATE TABLE dce_ip (i int PRIMARY KEY);
CREATE TABLE dce_ic (i int PRIMARY KEY, p int,
                     CONSTRAINT dce_if FOREIGN KEY (p) REFERENCES dce_ip(i)
                     DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_ip VALUES (1);
INSERT INTO dce_ic VALUES (1,1);
BEGIN;
DELETE FROM dce_ip WHERE i = 1;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "dce_ip" violates foreign key constraint "dce_if"
-- end-expected-error
SET CONSTRAINTS dce_if IMMEDIATE;
ROLLBACK;

-- afterwards the delete fires at the statement
BEGIN;
SET CONSTRAINTS ALL IMMEDIATE;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "dce_ip" violates foreign key constraint "dce_if"
-- end-expected-error
DELETE FROM dce_ip WHERE i = 1;
ROLLBACK;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_ip;

-- named, schema-qualified and ALL forms of SET CONSTRAINTS
DROP TABLE IF EXISTS dce_sn CASCADE;
CREATE TABLE dce_sn (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_snu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_sn VALUES (1,1);
BEGIN;
SET CONSTRAINTS dce_snu IMMEDIATE;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_snu"
-- end-expected-error
INSERT INTO dce_sn VALUES (2,1);
ROLLBACK;

BEGIN;
SET CONSTRAINTS public.dce_snu IMMEDIATE;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_snu"
-- end-expected-error
INSERT INTO dce_sn VALUES (3,1);
ROLLBACK;

BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO dce_sn VALUES (4,1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_snu"
-- end-expected-error
SET CONSTRAINTS ALL IMMEDIATE;
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_snu"
-- end-expected-error
INSERT INTO dce_sn VALUES (5,1);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "dce_no_such_constraint" does not exist
-- end-expected-error
SET CONSTRAINTS dce_no_such_constraint DEFERRED;

-- ============================================================================
-- 9. RESTRICT refuses the statement even when the constraint is deferred
-- ============================================================================
DROP TABLE IF EXISTS dce_sc CASCADE;
DROP TABLE IF EXISTS dce_sp2 CASCADE;
CREATE TABLE dce_sp2 (i int PRIMARY KEY);
CREATE TABLE dce_sc (i int PRIMARY KEY, p int,
                     CONSTRAINT dce_sf FOREIGN KEY (p) REFERENCES dce_sp2(i)
                     ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_sp2 VALUES (1);
INSERT INTO dce_sc VALUES (1,1);
BEGIN;

-- begin-expected-error
-- sqlstate: 23001
-- message-like: violates RESTRICT setting of foreign key constraint "dce_sf"
-- end-expected-error
DELETE FROM dce_sp2 WHERE i = 1;
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23001
-- message-like: violates RESTRICT setting of foreign key constraint "dce_sf"
-- end-expected-error
UPDATE dce_sp2 SET i = 2 WHERE i = 1;

-- a non-deferrable key reports the plain NO ACTION wording
DROP TABLE IF EXISTS dce_nc CASCADE;
DROP TABLE IF EXISTS dce_np CASCADE;
CREATE TABLE dce_np (i int PRIMARY KEY);
CREATE TABLE dce_nc (i int PRIMARY KEY, p int REFERENCES dce_np(i));
INSERT INTO dce_np VALUES (1);
INSERT INTO dce_nc VALUES (1,1);
BEGIN;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "dce_np" violates foreign key constraint
-- end-expected-error
DELETE FROM dce_np WHERE i = 1;
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "dce_np" violates foreign key constraint
-- end-expected-error
UPDATE dce_np SET i = 2 WHERE i = 1;

-- ============================================================================
-- 10. CASCADE and SET NULL still act at the statement when deferred
-- ============================================================================
DROP TABLE IF EXISTS dce_cc CASCADE;
DROP TABLE IF EXISTS dce_cn CASCADE;
DROP TABLE IF EXISTS dce_cp CASCADE;
CREATE TABLE dce_cp (i int PRIMARY KEY);
CREATE TABLE dce_cc (i int PRIMARY KEY, p int,
                     CONSTRAINT dce_cf FOREIGN KEY (p) REFERENCES dce_cp(i)
                     ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE dce_cn (i int PRIMARY KEY, p int,
                     CONSTRAINT dce_nf FOREIGN KEY (p) REFERENCES dce_cp(i)
                     ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_cp VALUES (1);
INSERT INTO dce_cc VALUES (1,1);
INSERT INTO dce_cn VALUES (1,1);
BEGIN;
DELETE FROM dce_cp WHERE i = 1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dce_cc;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dce_cn WHERE p IS NOT NULL;
COMMIT;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dce_cc;

-- ============================================================================
-- 11. What deferring is for, and must keep doing
-- ============================================================================
DROP TABLE IF EXISTS dce_sw CASCADE;
CREATE TABLE dce_sw (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_swu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_sw VALUES (1,1),(2,2);
BEGIN;
UPDATE dce_sw SET j = 2 WHERE i = 1;
UPDATE dce_sw SET j = 1 WHERE i = 2;
COMMIT;

-- begin-expected
-- columns: j
-- row: 2
-- row: 1
-- end-expected
SELECT j::text AS j FROM dce_sw ORDER BY i;

-- a duplicate written and then removed before COMMIT is fine
BEGIN;
INSERT INTO dce_sw VALUES (3,2);
DELETE FROM dce_sw WHERE i = 1;
COMMIT;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM dce_sw;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_swu"
-- end-expected-error
INSERT INTO dce_sw VALUES (4,2);

-- a child row written before its parent
DROP TABLE IF EXISTS dce_oc CASCADE;
DROP TABLE IF EXISTS dce_op CASCADE;
CREATE TABLE dce_op (i int PRIMARY KEY);
CREATE TABLE dce_oc (i int PRIMARY KEY, p int,
                     CONSTRAINT dce_of FOREIGN KEY (p) REFERENCES dce_op(i)
                     DEFERRABLE INITIALLY DEFERRED);
BEGIN;
INSERT INTO dce_oc VALUES (1,5);
INSERT INTO dce_op VALUES (5);
COMMIT;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_oc;

-- a self-referencing deferred key, both directions written in one transaction
DROP TABLE IF EXISTS dce_self CASCADE;
CREATE TABLE dce_self (i int PRIMARY KEY, parent int,
                       CONSTRAINT dce_selff FOREIGN KEY (parent) REFERENCES dce_self(i)
                       DEFERRABLE INITIALLY DEFERRED);
BEGIN;
INSERT INTO dce_self VALUES (1,2);
INSERT INTO dce_self VALUES (2,1);
COMMIT;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM dce_self;

BEGIN;
DELETE FROM dce_self WHERE i = 1;

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "dce_self" violates foreign key constraint "dce_selff"
-- end-expected-error
COMMIT;

-- ============================================================================
-- 12. NULLs under a deferred UNIQUE remain distinct
-- ============================================================================
DROP TABLE IF EXISTS dce_nn CASCADE;
CREATE TABLE dce_nn (i int PRIMARY KEY, j int,
                     CONSTRAINT dce_nnu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_nn VALUES (1,NULL),(2,NULL);
BEGIN;
INSERT INTO dce_nn VALUES (3,NULL);
ROLLBACK;
INSERT INTO dce_nn VALUES (4,NULL);

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::text AS n FROM dce_nn;

-- ============================================================================
-- 13. A plain constraint is untouched by a nearby aborted transaction
-- ============================================================================
DROP TABLE IF EXISTS dce_pl CASCADE;
CREATE TABLE dce_pl (i int PRIMARY KEY, j int UNIQUE);
INSERT INTO dce_pl VALUES (1,1);
BEGIN;
INSERT INTO dce_pl VALUES (2,2);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO dce_pl VALUES (3,1);
INSERT INTO dce_pl VALUES (4,4);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM dce_pl;

-- a unique index created separately is not deferrable at all
DROP TABLE IF EXISTS dce_ix CASCADE;
CREATE TABLE dce_ix (i int PRIMARY KEY, j int);
CREATE UNIQUE INDEX dce_ixu ON dce_ix (j);
INSERT INTO dce_ix VALUES (1,1);
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO dce_ix VALUES (2,1);
ROLLBACK;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO dce_ix VALUES (3,1);

-- ============================================================================
-- 14. Declaring deferrability
-- ============================================================================
DROP TABLE IF EXISTS dce_ck CASCADE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: CHECK constraints cannot be marked DEFERRABLE
-- end-expected-error
CREATE TABLE dce_ck (i int, CONSTRAINT dce_ckc CHECK (i > 0) DEFERRABLE INITIALLY DEFERRED);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: CHECK constraints cannot be marked DEFERRABLE
-- end-expected-error
CREATE TABLE dce_ck (i int, CONSTRAINT dce_ckc CHECK (i > 0) DEFERRABLE);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: CHECK constraints cannot be marked DEFERRABLE
-- end-expected-error
CREATE TABLE dce_ck (i int, CONSTRAINT dce_ckc CHECK (i > 0) INITIALLY DEFERRED);

-- on a column there is nothing for the clause to attach to
-- begin-expected-error
-- sqlstate: 42601
-- message-like: misplaced DEFERRABLE clause
-- end-expected-error
CREATE TABLE dce_ck (i int CHECK (i > 0) DEFERRABLE);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: misplaced NOT DEFERRABLE clause
-- end-expected-error
CREATE TABLE dce_ck (i int CHECK (i > 0) NOT DEFERRABLE);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: misplaced INITIALLY IMMEDIATE clause
-- end-expected-error
CREATE TABLE dce_ck (i int CHECK (i > 0) INITIALLY IMMEDIATE);

-- what a CHECK already is, it may still say
CREATE TABLE dce_ck (i int, CONSTRAINT dce_ckc CHECK (i > 0) NOT DEFERRABLE INITIALLY IMMEDIATE);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: CHECK constraints cannot be marked DEFERRABLE
-- end-expected-error
ALTER TABLE dce_ck ADD CONSTRAINT dce_ckd CHECK (i < 100) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE dce_ck ADD CONSTRAINT dce_cke CHECK (i < 100) NOT DEFERRABLE;
INSERT INTO dce_ck VALUES (5);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "dce_ckc"
-- end-expected-error
INSERT INTO dce_ck VALUES (-5);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_ck;

-- every combination of the clauses on the kinds that take them
DROP TABLE IF EXISTS dce_dc CASCADE;
CREATE TABLE dce_dc (i int, j int, k int, m int,
                     CONSTRAINT dce_dcp PRIMARY KEY (i) NOT DEFERRABLE,
                     CONSTRAINT dce_dca UNIQUE (j) DEFERRABLE INITIALLY IMMEDIATE,
                     CONSTRAINT dce_dcb UNIQUE (k) NOT DEFERRABLE INITIALLY IMMEDIATE,
                     CONSTRAINT dce_dcc UNIQUE (m) INITIALLY DEFERRED);
INSERT INTO dce_dc VALUES (1,1,1,1);

-- begin-expected
-- columns: constraint_name | is_deferrable | initially_deferred
-- row: dce_dca, YES, NO
-- row: dce_dcb, NO, NO
-- row: dce_dcc, YES, YES
-- row: dce_dcp, NO, NO
-- end-expected
SELECT constraint_name, is_deferrable, initially_deferred
FROM information_schema.table_constraints
WHERE constraint_name IN ('dce_dcp','dce_dca','dce_dcb','dce_dcc')
ORDER BY constraint_name;

-- begin-expected
-- columns: conname | condeferrable | condeferred
-- row: dce_dca, true, false
-- row: dce_dcb, false, false
-- row: dce_dcc, true, true
-- row: dce_dcp, false, false
-- end-expected
SELECT conname, condeferrable::text AS condeferrable, condeferred::text AS condeferred
FROM pg_constraint WHERE conname IN ('dce_dcp','dce_dca','dce_dcb','dce_dcc')
ORDER BY conname;

-- the bare INITIALLY DEFERRED really does defer
BEGIN;
INSERT INTO dce_dc VALUES (2,2,2,1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "dce_dcc"
-- end-expected-error
COMMIT;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dce_dc;

-- a column-level key and reference take their deferrability too
DROP TABLE IF EXISTS dce_clc CASCADE;
DROP TABLE IF EXISTS dce_clp CASCADE;
CREATE TABLE dce_clp (i int PRIMARY KEY NOT DEFERRABLE);
CREATE TABLE dce_clc (i int PRIMARY KEY,
                      j int UNIQUE DEFERRABLE INITIALLY DEFERRED,
                      p int REFERENCES dce_clp(i) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO dce_clp VALUES (1);
BEGIN;
INSERT INTO dce_clc VALUES (1,1,9);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
COMMIT;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dce_clc;
INSERT INTO dce_clc VALUES (1,1,1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO dce_clc VALUES (2,1,1);

-- ============================================================================
-- cleanup
-- ============================================================================
DROP TABLE IF EXISTS dce_clc CASCADE;
DROP TABLE IF EXISTS dce_clp CASCADE;
DROP TABLE IF EXISTS dce_dc CASCADE;
DROP TABLE IF EXISTS dce_ck CASCADE;
DROP TABLE IF EXISTS dce_ix CASCADE;
DROP TABLE IF EXISTS dce_pl CASCADE;
DROP TABLE IF EXISTS dce_nn CASCADE;
DROP TABLE IF EXISTS dce_self CASCADE;
DROP TABLE IF EXISTS dce_oc CASCADE;
DROP TABLE IF EXISTS dce_op CASCADE;
DROP TABLE IF EXISTS dce_sw CASCADE;
DROP TABLE IF EXISTS dce_cc CASCADE;
DROP TABLE IF EXISTS dce_cn CASCADE;
DROP TABLE IF EXISTS dce_cp CASCADE;
DROP TABLE IF EXISTS dce_nc CASCADE;
DROP TABLE IF EXISTS dce_np CASCADE;
DROP TABLE IF EXISTS dce_sc CASCADE;
DROP TABLE IF EXISTS dce_sp2 CASCADE;
DROP TABLE IF EXISTS dce_sn CASCADE;
DROP TABLE IF EXISTS dce_ic CASCADE;
DROP TABLE IF EXISTS dce_ip CASCADE;
DROP TABLE IF EXISTS dce_kc CASCADE;
DROP TABLE IF EXISTS dce_kp CASCADE;
DROP TABLE IF EXISTS dce_rc CASCADE;
DROP TABLE IF EXISTS dce_rp CASCADE;
DROP TABLE IF EXISTS dce_fkc CASCADE;
DROP TABLE IF EXISTS dce_fkp CASCADE;
DROP TABLE IF EXISTS dce_sp CASCADE;
DROP TABLE IF EXISTS dce_ii CASCADE;
DROP TABLE IF EXISTS dce_ta CASCADE;
DROP TABLE IF EXISTS dce_tb CASCADE;
DROP TABLE IF EXISTS dce_two CASCADE;
DROP TABLE IF EXISTS dce_mc CASCADE;
DROP TABLE IF EXISTS dce_pk CASCADE;
DROP TABLE IF EXISTS dce_up CASCADE;
DROP TABLE IF EXISTS dce_u CASCADE;
