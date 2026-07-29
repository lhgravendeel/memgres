-- ============================================================================
-- Feature Comparison: sessions, transactions, cursors and locks (round 2)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers:
--   1. which statements leave a transaction free to choose its isolation level
--   2. setval and ALTER SEQUENCE RESTART give up a session's cached block
--   3. a column-level CONSTRAINT clause names the constraint it introduces
--   4. SET CONSTRAINTS takes a schema-qualified name
--   5. FOR UPDATE on a derived table over the nullable side of an outer join
--   6. FOR UPDATE in a read-only transaction
--   7. the transaction_mode grammar refuses what it does not understand
--   8. a quoted savepoint name keeps its case
--   9. transaction-scoped settings cannot be reset
-- ============================================================================

-- ============================================================================
-- 1. Statements that take no snapshot, and statements that do
-- ============================================================================

-- LISTEN, NOTIFY, UNLISTEN and CHECKPOINT run without a snapshot, so the
-- isolation level is still open afterwards.
BEGIN;
LISTEN r10_chan;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: transaction_isolation
-- row: serializable
-- end-expected
SHOW transaction_isolation;

ROLLBACK;

BEGIN;
NOTIFY r10_chan;

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: transaction_isolation
-- row: repeatable read
-- end-expected
SHOW transaction_isolation;

ROLLBACK;

BEGIN;
UNLISTEN r10_chan;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: transaction_isolation
-- row: serializable
-- end-expected
SHOW transaction_isolation;

ROLLBACK;

UNLISTEN *;

BEGIN;
CHECKPOINT;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: transaction_isolation
-- row: serializable
-- end-expected
SHOW transaction_isolation;

ROLLBACK;

-- DEALLOCATE and DISCARD are not on that list: they fix the snapshot like
-- any other statement.
BEGIN;
DEALLOCATE ALL;

-- begin-expected-error
-- error: 25001
-- message-like: must be called before any query
-- end-expected-error
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

ROLLBACK;

BEGIN;
DISCARD PLANS;

-- begin-expected-error
-- error: 25001
-- message-like: must be called before any query
-- end-expected-error
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

ROLLBACK;

-- A setting, a SHOW and a LOCK still take no snapshot.
DROP TABLE IF EXISTS r10_snap;
CREATE TABLE r10_snap (i int primary key);

BEGIN;
SET work_mem = '4MB';
SHOW work_mem;
LOCK TABLE r10_snap;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: transaction_isolation
-- row: serializable
-- end-expected
SHOW transaction_isolation;

ROLLBACK;

-- Reading a table does take one.
BEGIN;
SELECT count(*) FROM r10_snap;

-- begin-expected-error
-- error: 25001
-- message-like: must be called before any query
-- end-expected-error
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

ROLLBACK;
DROP TABLE IF EXISTS r10_snap;

-- ============================================================================
-- 2. A cached CACHE block is given up when the counter is moved
-- ============================================================================

DROP SEQUENCE IF EXISTS r10_seq_restart;
CREATE SEQUENCE r10_seq_restart CACHE 3;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT nextval('r10_seq_restart') AS v;

ALTER SEQUENCE r10_seq_restart RESTART WITH 100;

-- begin-expected
-- columns: v
-- row: 100
-- end-expected
SELECT nextval('r10_seq_restart') AS v;

-- begin-expected
-- columns: v
-- row: 101
-- end-expected
SELECT nextval('r10_seq_restart') AS v;

DROP SEQUENCE IF EXISTS r10_seq_restart;

DROP SEQUENCE IF EXISTS r10_seq_setval;
CREATE SEQUENCE r10_seq_setval CACHE 4;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT nextval('r10_seq_setval') AS v;

SELECT setval('r10_seq_setval', 50);

-- begin-expected
-- columns: v
-- row: 51
-- end-expected
SELECT nextval('r10_seq_setval') AS v;

-- currval reports what this session last drew, which is the value after the setval
-- begin-expected
-- columns: v
-- row: 51
-- end-expected
SELECT currval('r10_seq_setval') AS v;

SELECT setval('r10_seq_setval', 60, false);

-- begin-expected
-- columns: v
-- row: 60
-- end-expected
SELECT nextval('r10_seq_setval') AS v;

DROP SEQUENCE IF EXISTS r10_seq_setval;

-- ============================================================================
-- 3. A column-level CONSTRAINT clause names the constraint
-- ============================================================================

DROP TABLE IF EXISTS r10_named_child;
DROP TABLE IF EXISTS r10_named_parent;
CREATE TABLE r10_named_parent (id int primary key);
CREATE TABLE r10_named_child (
    id int CONSTRAINT r10_child_pk PRIMARY KEY,
    p int CONSTRAINT r10_child_fk REFERENCES r10_named_parent(id) DEFERRABLE INITIALLY DEFERRED,
    q int CONSTRAINT r10_child_ck CHECK (q > 0),
    r int CONSTRAINT r10_child_uq UNIQUE
);

-- begin-expected
-- columns: conname
-- row: r10_child_ck
-- row: r10_child_fk
-- row: r10_child_pk
-- row: r10_child_uq
-- row: r10_named_child_id_not_null
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'r10_named_child'::regclass ORDER BY conname;

BEGIN;

-- The name really is there, so deferring it is not a request for a constraint
-- that does not exist.
SET CONSTRAINTS r10_child_fk DEFERRED;
SET CONSTRAINTS r10_child_fk IMMEDIATE;

-- A schema-qualified name is split on the dot, not looked up whole.
SET CONSTRAINTS public.r10_child_fk DEFERRED;
SET CONSTRAINTS public.r10_child_fk IMMEDIATE;

-- A key that is not deferrable says so, rather than claiming not to exist.
-- begin-expected-error
-- error: 42809
-- message-like: is not deferrable
-- end-expected-error
SET CONSTRAINTS r10_child_pk DEFERRED;

ROLLBACK;

-- A name that really is absent is still reported.
-- begin-expected-error
-- error: 42704
-- message-like: does not exist
-- end-expected-error
SET CONSTRAINTS r10_no_such_constraint DEFERRED;

-- And a schema that is absent is reported as a schema.
-- begin-expected-error
-- error: 3F000
-- message-like: schema "r10_no_schema" does not exist
-- end-expected-error
SET CONSTRAINTS r10_no_schema.r10_child_fk DEFERRED;

-- The generated names are unchanged where no CONSTRAINT clause was written.
DROP TABLE IF EXISTS r10_gen_child;
CREATE TABLE r10_gen_child (
    a int primary key,
    b int unique,
    c int references r10_named_parent(id),
    d int check (d > 0)
);

-- begin-expected
-- columns: conname
-- row: r10_gen_child_a_not_null
-- row: r10_gen_child_b_key
-- row: r10_gen_child_c_fkey
-- row: r10_gen_child_d_check
-- row: r10_gen_child_pkey
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'r10_gen_child'::regclass ORDER BY conname;

DROP TABLE IF EXISTS r10_gen_child;
DROP TABLE IF EXISTS r10_named_child;
DROP TABLE IF EXISTS r10_named_parent;

-- ============================================================================
-- 4. FOR UPDATE reaches through a derived table
-- ============================================================================

DROP TABLE IF EXISTS r10_lk_left;
DROP TABLE IF EXISTS r10_lk_right;
CREATE TABLE r10_lk_left (id int primary key, v int);
CREATE TABLE r10_lk_right (id int primary key, l1 int);
INSERT INTO r10_lk_left VALUES (1, 10), (2, 20);
INSERT INTO r10_lk_right VALUES (1, 1);

-- A sub-select on the nullable side has no base row behind an all-NULL output row.
-- begin-expected-error
-- error: 0A000
-- message-like: nullable side of an outer join
-- end-expected-error
SELECT a.id FROM r10_lk_left a
    LEFT JOIN (SELECT * FROM r10_lk_right) s ON s.l1 = a.id
    FOR UPDATE;

-- begin-expected-error
-- error: 0A000
-- message-like: nullable side of an outer join
-- end-expected-error
SELECT a.id FROM r10_lk_left a
    LEFT JOIN LATERAL (SELECT * FROM r10_lk_right c WHERE c.l1 = a.id) z ON true
    FOR UPDATE;

-- Naming the other side is fine, and so is an inner join.
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT a.id FROM r10_lk_left a
    LEFT JOIN (SELECT * FROM r10_lk_right) s ON s.l1 = a.id
    ORDER BY a.id FOR UPDATE OF a;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT a.id FROM r10_lk_left a
    JOIN (SELECT * FROM r10_lk_right) s ON s.l1 = a.id
    ORDER BY a.id FOR UPDATE;

-- A set-returning function on the nullable side is not a relation to lock, and a
-- plain FOR UPDATE passes over it rather than refusing.
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT a.id FROM r10_lk_left a LEFT JOIN generate_series(1, 2) g ON true
    WHERE g = 1 ORDER BY a.id FOR UPDATE;

-- OF names a FROM entry, which a schema cannot qualify.
-- begin-expected-error
-- error: 42601
-- message-like: must specify unqualified relation names
-- end-expected-error
SELECT id FROM r10_lk_left ORDER BY id FOR UPDATE OF public.r10_lk_left;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM r10_lk_left ORDER BY id FOR UPDATE OF r10_lk_left;

-- ============================================================================
-- 5. FOR UPDATE in a read-only transaction
-- ============================================================================

BEGIN READ ONLY;

-- begin-expected-error
-- error: 25006
-- message-like: cannot execute SELECT FOR UPDATE in a read-only transaction
-- end-expected-error
SELECT id FROM r10_lk_left FOR UPDATE;

ROLLBACK;

BEGIN READ ONLY;

-- begin-expected-error
-- error: 25006
-- message-like: cannot execute SELECT FOR SHARE in a read-only transaction
-- end-expected-error
SELECT id FROM r10_lk_left FOR SHARE;

ROLLBACK;

BEGIN READ ONLY;

-- begin-expected-error
-- error: 25006
-- message-like: cannot execute SELECT FOR KEY SHARE in a read-only transaction
-- end-expected-error
SELECT id FROM r10_lk_left FOR KEY SHARE;

ROLLBACK;

BEGIN READ ONLY;

-- begin-expected-error
-- error: 25006
-- message-like: cannot execute SELECT FOR NO KEY UPDATE in a read-only transaction
-- end-expected-error
SELECT id FROM r10_lk_left FOR NO KEY UPDATE;

ROLLBACK;

BEGIN READ ONLY;

-- A plain read is still a read.
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM r10_lk_left ORDER BY id;

ROLLBACK;

-- A read-write transaction still takes the lock.
BEGIN;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM r10_lk_left ORDER BY id FOR UPDATE;

COMMIT;

DROP TABLE IF EXISTS r10_lk_left;
DROP TABLE IF EXISTS r10_lk_right;

-- ============================================================================
-- 6. The transaction_mode grammar refuses what it does not understand
-- ============================================================================

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "garbage"
-- end-expected-error
BEGIN garbage;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "NONSENSE"
-- end-expected-error
SET TRANSACTION ISOLATION LEVEL NONSENSE;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "NONSENSE"
-- end-expected-error
BEGIN ISOLATION LEVEL NONSENSE;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "WRITE"
-- end-expected-error
BEGIN ISOLATION LEVEL READ WRITE;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at end of input
-- end-expected-error
BEGIN READ;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET TRANSACTION;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET SESSION CHARACTERISTICS AS TRANSACTION;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "NONSENSE"
-- end-expected-error
SET SESSION CHARACTERISTICS AS TRANSACTION NONSENSE;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "garbage"
-- end-expected-error
START TRANSACTION garbage;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "garbage"
-- end-expected-error
COMMIT garbage;

-- begin-expected-error
-- error: 42601
-- message-like: syntax error at or near "garbage"
-- end-expected-error
ROLLBACK garbage;

-- The spellings that are transaction modes still parse.
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ WRITE, NOT DEFERRABLE;
COMMIT;
START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY;
COMMIT;
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
COMMIT;
SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- ============================================================================
-- 7. A quoted savepoint name keeps its case
-- ============================================================================

BEGIN;
SAVEPOINT "R10Sp";

-- begin-expected-error
-- error: 3B001
-- message-like: savepoint "r10sp" does not exist
-- end-expected-error
ROLLBACK TO SAVEPOINT r10sp;

ROLLBACK;

BEGIN;
SAVEPOINT "R10Sp";
ROLLBACK TO SAVEPOINT "R10Sp";
RELEASE SAVEPOINT "R10Sp";
ROLLBACK;

-- An unquoted name still folds, so any spelling of it finds the savepoint.
BEGIN;
SAVEPOINT R10Sp2;
ROLLBACK TO SAVEPOINT r10sp2;
RELEASE SAVEPOINT R10SP2;
ROLLBACK;

-- ============================================================================
-- 8. Transaction-scoped settings
-- ============================================================================

-- begin-expected-error
-- error: 0A000
-- message-like: cannot be reset
-- end-expected-error
RESET transaction_read_only;

-- begin-expected-error
-- error: 0A000
-- message-like: cannot be reset
-- end-expected-error
RESET transaction_isolation;

-- set_config runs inside a query, so the isolation level can no longer be chosen.
-- begin-expected-error
-- error: 25001
-- message-like: must be called before any query
-- end-expected-error
SELECT set_config('transaction_isolation', 'serializable', false);

-- Setting one with no transaction open changes nothing that outlives the statement.
SELECT set_config('transaction_read_only', 'on', false);

-- begin-expected
-- columns: transaction_read_only
-- row: off
-- end-expected
SHOW transaction_read_only;

-- An ordinary setting is unaffected.
SELECT set_config('work_mem', '5MB', false);

-- begin-expected
-- columns: work_mem
-- row: 5MB
-- end-expected
SHOW work_mem;

RESET work_mem;

-- ============================================================================
-- 9. The blocking advisory lock functions return void
-- ============================================================================

-- begin-expected
-- columns: pg_typeof
-- row: void
-- end-expected
SELECT pg_typeof(pg_advisory_unlock_all());

-- begin-expected
-- columns: pg_typeof
-- row: void
-- end-expected
SELECT pg_typeof(pg_advisory_lock(9100010));

SELECT pg_advisory_unlock_all();
