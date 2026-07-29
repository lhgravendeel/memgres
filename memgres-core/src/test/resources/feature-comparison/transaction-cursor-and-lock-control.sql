-- ============================================================================
-- Feature Comparison: sessions, transactions, cursors and locks
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers:
--   1. CACHE is an allocation hint, not a claim against the sequence's bounds
--   2. an explicit READ WRITE overrides default_transaction_read_only
--   3. SET CONSTRAINTS ... IMMEDIATE runs the checks it has postponed
--   4. savepoints are a stack of names, not a set of them
--   5. ROLLBACK TO SAVEPOINT destroys cursors opened inside the subtransaction
--   6. FOR UPDATE legality: OF names, aliases, nullable outer-join sides
--   7. transaction-control statements outside a transaction block
-- ============================================================================

-- ============================================================================
-- 1. Sequence CACHE wider than the sequence's range
-- ============================================================================

DROP SEQUENCE IF EXISTS t10_cache_seq;
CREATE SEQUENCE t10_cache_seq MAXVALUE 3 CACHE 10;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT nextval('t10_cache_seq') AS v;

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT nextval('t10_cache_seq') AS v;

-- begin-expected
-- columns: v
-- row: 3
-- end-expected
SELECT nextval('t10_cache_seq') AS v;

-- the fourth call is the one that passes the bound
-- begin-expected-error
-- error: 2200H
-- message-like: reached maximum value of sequence
-- end-expected-error
SELECT nextval('t10_cache_seq');

DROP SEQUENCE t10_cache_seq;

-- a descending sequence whose cache is wider than its range behaves the same way
DROP SEQUENCE IF EXISTS t10_cache_desc;
CREATE SEQUENCE t10_cache_desc INCREMENT -1 MINVALUE -3 MAXVALUE -1 START -1 CACHE 10;

-- begin-expected
-- columns: a | b | c
-- row: -1, -2, -3
-- end-expected
SELECT nextval('t10_cache_desc') AS a,
       nextval('t10_cache_desc') AS b,
       nextval('t10_cache_desc') AS c;

-- begin-expected-error
-- error: 2200H
-- message-like: reached minimum value of sequence
-- end-expected-error
SELECT nextval('t10_cache_desc');

DROP SEQUENCE t10_cache_desc;

-- setval reports the bounds it was asked to leave
DROP SEQUENCE IF EXISTS t10_setval_seq;
CREATE SEQUENCE t10_setval_seq MAXVALUE 2;

-- begin-expected-error
-- error: 22003
-- message-like: is out of bounds for sequence
-- end-expected-error
SELECT setval('t10_setval_seq', 5);

DROP SEQUENCE t10_setval_seq;

-- ============================================================================
-- 2. An explicit READ WRITE overrides default_transaction_read_only
-- ============================================================================

DROP TABLE IF EXISTS t10_rw CASCADE;
CREATE TABLE t10_rw (id int PRIMARY KEY);

SET default_transaction_read_only = on;

-- begin-expected
-- columns: transaction_read_only
-- row: on
-- end-expected
SHOW transaction_read_only;

BEGIN READ WRITE;

-- begin-expected
-- columns: transaction_read_only
-- row: off
-- end-expected
SHOW transaction_read_only;

INSERT INTO t10_rw VALUES (1);
COMMIT;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM t10_rw;

-- SET TRANSACTION READ WRITE inside the block overrides it too
BEGIN;
SET TRANSACTION READ WRITE;
INSERT INTO t10_rw VALUES (2);
COMMIT;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM t10_rw;

-- without the override the session default still stops the write
BEGIN;

-- begin-expected-error
-- error: 25006
-- message-like: read-only transaction
-- end-expected-error
INSERT INTO t10_rw VALUES (3);

ROLLBACK;

SET default_transaction_read_only = off;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM t10_rw;

DROP TABLE t10_rw;

-- ============================================================================
-- 3. SET CONSTRAINTS ... IMMEDIATE runs the checks it has postponed
-- ============================================================================

DROP TABLE IF EXISTS t10_dc CASCADE;
CREATE TABLE t10_dc (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED);

BEGIN;
INSERT INTO t10_dc VALUES (1);
INSERT INTO t10_dc VALUES (1);

-- begin-expected-error
-- error: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
SET CONSTRAINTS ALL IMMEDIATE;

ROLLBACK;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*) AS cnt FROM t10_dc;

-- with nothing pending, SET CONSTRAINTS ALL IMMEDIATE simply switches the mode
BEGIN;
INSERT INTO t10_dc VALUES (1);
SET CONSTRAINTS ALL IMMEDIATE;

-- begin-expected-error
-- error: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO t10_dc VALUES (1);

ROLLBACK;

DROP TABLE t10_dc;

-- a deferred foreign key really is deferred
DROP TABLE IF EXISTS t10_child CASCADE;
DROP TABLE IF EXISTS t10_parent CASCADE;
CREATE TABLE t10_parent (id int PRIMARY KEY);
CREATE TABLE t10_child (id int PRIMARY KEY,
                        p int REFERENCES t10_parent(id) DEFERRABLE INITIALLY DEFERRED);

BEGIN;
INSERT INTO t10_child VALUES (1, 77);
INSERT INTO t10_parent VALUES (77);
COMMIT;

-- begin-expected
-- columns: p
-- row: 77
-- end-expected
SELECT p FROM t10_child ORDER BY p;

-- and SET CONSTRAINTS ALL IMMEDIATE brings its check forward
BEGIN;
INSERT INTO t10_child VALUES (2, 88);

-- begin-expected-error
-- error: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
SET CONSTRAINTS ALL IMMEDIATE;

ROLLBACK;

DROP TABLE t10_child;
DROP TABLE t10_parent;

-- naming a constraint that is not there, and one that cannot be deferred
DROP TABLE IF EXISTS t10_nd CASCADE;
CREATE TABLE t10_nd (id int, CONSTRAINT t10_nd_pk PRIMARY KEY (id));

BEGIN;

-- begin-expected-error
-- error: 42704
-- message-like: does not exist
-- end-expected-error
SET CONSTRAINTS t10_no_such_constraint DEFERRED;

ROLLBACK;
BEGIN;

-- begin-expected-error
-- error: 42809
-- message-like: is not deferrable
-- end-expected-error
SET CONSTRAINTS t10_nd_pk DEFERRED;

ROLLBACK;

-- IMMEDIATE is what a non-deferrable constraint already is, so it is accepted
BEGIN;
SET CONSTRAINTS t10_nd_pk IMMEDIATE;
ROLLBACK;

DROP TABLE t10_nd;

-- ============================================================================
-- 4. Savepoints are a stack of names, not a set of them
-- ============================================================================

DROP TABLE IF EXISTS t10_sp CASCADE;
CREATE TABLE t10_sp (i int PRIMARY KEY);

BEGIN;
INSERT INTO t10_sp VALUES (1);
SAVEPOINT s;
INSERT INTO t10_sp VALUES (2);
SAVEPOINT s;
INSERT INTO t10_sp VALUES (3);

-- releases the inner s, uncovering the outer one
RELEASE SAVEPOINT s;
ROLLBACK TO SAVEPOINT s;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i FROM t10_sp ORDER BY i;

COMMIT;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i FROM t10_sp ORDER BY i;

-- releasing a savepoint also destroys every savepoint established after it
BEGIN;
SAVEPOINT a;
SAVEPOINT b;
RELEASE SAVEPOINT a;

-- begin-expected-error
-- error: 3B001
-- message-like: does not exist
-- end-expected-error
ROLLBACK TO SAVEPOINT b;

ROLLBACK;

-- An aborted transaction accepts ROLLBACK TO SAVEPOINT but not SAVEPOINT, which is asserted in
-- TransactionControlTest rather than here: this harness ends the block when a statement errors,
-- so the aborted state never survives to the next statement and both engines answer the later
-- SAVEPOINT with 25P01 instead. What can be shown here is that the savepoint stack works.
BEGIN;
SAVEPOINT ok1;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT 1 AS v;

ROLLBACK TO SAVEPOINT ok1;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT 1 AS v;

COMMIT;

DROP TABLE t10_sp;

-- ============================================================================
-- 5. ROLLBACK TO SAVEPOINT destroys cursors opened inside the subtransaction
-- ============================================================================

DROP TABLE IF EXISTS t10_cur CASCADE;
CREATE TABLE t10_cur (i int PRIMARY KEY);
INSERT INTO t10_cur VALUES (1),(2),(3);

BEGIN;
SAVEPOINT s1;
DECLARE t10_c CURSOR FOR SELECT i FROM t10_cur ORDER BY i;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
FETCH 1 FROM t10_c;

ROLLBACK TO SAVEPOINT s1;

-- begin-expected-error
-- error: 34000
-- message-like: does not exist
-- end-expected-error
FETCH 1 FROM t10_c;

ROLLBACK;

-- a cursor opened before the savepoint survives, at the position FETCH left it
BEGIN;
DECLARE t10_c2 CURSOR FOR SELECT i FROM t10_cur ORDER BY i;
SAVEPOINT s1;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
FETCH 1 FROM t10_c2;

ROLLBACK TO SAVEPOINT s1;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
FETCH 1 FROM t10_c2;

ROLLBACK;

-- a cursor opened inside a subtransaction that was released stays alive
BEGIN;
SAVEPOINT s1;
DECLARE t10_c3 CURSOR FOR SELECT i FROM t10_cur ORDER BY i;
RELEASE SAVEPOINT s1;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
FETCH 1 FROM t10_c3;

ROLLBACK;

DROP TABLE t10_cur;

-- ============================================================================
-- 6. FOR UPDATE legality
-- ============================================================================

DROP TABLE IF EXISTS t10_fu CASCADE;
CREATE TABLE t10_fu (id int PRIMARY KEY, v int);
INSERT INTO t10_fu VALUES (1,10),(2,20);

-- a name in OF that is not in FROM
-- begin-expected-error
-- error: 42P01
-- message-like: in FOR UPDATE clause not found in FROM clause
-- end-expected-error
SELECT id FROM t10_fu FOR UPDATE OF t10_nosuch;

-- the mode is reported as written
-- begin-expected-error
-- error: 42P01
-- message-like: in FOR SHARE clause not found in FROM clause
-- end-expected-error
SELECT id FROM t10_fu FOR SHARE OF t10_nosuch;

-- an alias hides the relation name it stands for
-- begin-expected-error
-- error: 42P01
-- message-like: in FOR UPDATE clause not found in FROM clause
-- end-expected-error
SELECT a.id FROM t10_fu a FOR UPDATE OF t10_fu;

-- the nullable side of an outer join has no base row to lock
-- begin-expected-error
-- error: 0A000
-- message-like: nullable side of an outer join
-- end-expected-error
SELECT a.id FROM t10_fu a LEFT JOIN t10_fu b ON a.id = b.id FOR UPDATE OF b;

-- begin-expected-error
-- error: 0A000
-- message-like: nullable side of an outer join
-- end-expected-error
SELECT a.id FROM t10_fu a LEFT JOIN t10_fu b ON a.id = b.id ORDER BY a.id FOR UPDATE;

-- begin-expected-error
-- error: 0A000
-- message-like: nullable side of an outer join
-- end-expected-error
SELECT a.id FROM t10_fu a RIGHT JOIN t10_fu b ON a.id = b.id FOR UPDATE OF a;

-- naming a function in OF
-- begin-expected-error
-- error: 0A000
-- message-like: cannot be applied to a function
-- end-expected-error
SELECT * FROM generate_series(1,2) g FOR UPDATE OF g;

-- ---- and the ordinary shapes are still accepted ----

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM t10_fu ORDER BY id FOR UPDATE;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM t10_fu WHERE id = 1 FOR UPDATE;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM t10_fu ORDER BY id LIMIT 1 FOR UPDATE;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM t10_fu ORDER BY id FOR SHARE;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM t10_fu ORDER BY id FOR NO KEY UPDATE;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM t10_fu ORDER BY id FOR KEY SHARE;

-- the non-nullable side of an outer join is lockable by name
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT a.id FROM t10_fu a LEFT JOIN t10_fu b ON a.id = b.id ORDER BY a.id FOR UPDATE OF a;

-- an inner join is lockable whole
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT a.id FROM t10_fu a JOIN t10_fu b ON a.id = b.id ORDER BY a.id FOR UPDATE;

-- a subquery alias may be named in OF
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM (SELECT id FROM t10_fu) s ORDER BY id FOR UPDATE OF s;

-- a plain FOR UPDATE over a function locks nothing and is accepted
-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT g FROM generate_series(1,2) g ORDER BY g FOR UPDATE;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM t10_fu ORDER BY id FOR UPDATE NOWAIT;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM t10_fu ORDER BY id FOR UPDATE SKIP LOCKED;

DROP TABLE t10_fu;

-- ============================================================================
-- 7. Transaction-control statements outside a transaction block
-- ============================================================================

-- begin-expected-error
-- error: 25P01
-- message-like: can only be used in transaction blocks
-- end-expected-error
SAVEPOINT t10_outside;

-- begin-expected-error
-- error: 25P01
-- message-like: can only be used in transaction blocks
-- end-expected-error
RELEASE SAVEPOINT t10_outside;

-- begin-expected-error
-- error: 25P01
-- message-like: can only be used in transaction blocks
-- end-expected-error
ROLLBACK TO SAVEPOINT t10_outside;

-- begin-expected-error
-- error: 25P01
-- message-like: can only be used in transaction blocks
-- end-expected-error
COMMIT AND CHAIN;

-- begin-expected-error
-- error: 25P01
-- message-like: can only be used in transaction blocks
-- end-expected-error
ROLLBACK AND CHAIN;

-- a plain COMMIT or ROLLBACK outside a block is still only a warning
COMMIT;
ROLLBACK;

-- SET TRANSACTION must come before the transaction has taken its snapshot
BEGIN;
SELECT 1;

-- begin-expected-error
-- error: 25001
-- message-like: must be called before any query
-- end-expected-error
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

ROLLBACK;

-- outside a transaction block there is nothing to configure, so it is only a warning
SET TRANSACTION DEFERRABLE;

-- before any query it is accepted
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: transaction_isolation
-- row: serializable
-- end-expected
SHOW transaction_isolation;

ROLLBACK;

-- and an isolation level PostgreSQL does not have is a syntax error
-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near
-- end-expected-error
SET TRANSACTION ISOLATION LEVEL NONSENSE;

-- begin-expected-error
-- error: 22023
-- message-like: invalid value for parameter
-- end-expected-error
SET default_transaction_isolation = 'nonsense';

-- transaction_deferrable is a setting, and it starts off
-- begin-expected
-- columns: transaction_deferrable
-- row: off
-- end-expected
SHOW transaction_deferrable;

BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;

-- begin-expected
-- columns: transaction_deferrable
-- row: on
-- end-expected
SHOW transaction_deferrable;

COMMIT;

-- begin-expected
-- columns: transaction_deferrable
-- row: off
-- end-expected
SHOW transaction_deferrable;

-- SET SESSION CHARACTERISTICS still reaches the session defaults
SET SESSION CHARACTERISTICS AS TRANSACTION DEFERRABLE;

-- begin-expected
-- columns: default_transaction_deferrable
-- row: on
-- end-expected
SHOW default_transaction_deferrable;

SET SESSION CHARACTERISTICS AS TRANSACTION NOT DEFERRABLE;

-- begin-expected
-- columns: default_transaction_deferrable
-- row: off
-- end-expected
SHOW default_transaction_deferrable;
