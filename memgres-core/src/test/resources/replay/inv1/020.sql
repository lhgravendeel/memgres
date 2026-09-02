-- source: investigation.md
-- finding: 20
-- title: Deferred-constraint and savepoint control
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE t (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO t VALUES (1);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO t VALUES (1);
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "t_pkey"
-- end-expected-error
SET CONSTRAINTS ALL IMMEDIATE;
-- PG: 23505 fires the deferred check here | mg: OK, no check
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SAVEPOINT s;
-- begin-expected
-- ok: 0
-- end-expected
SAVEPOINT s;
-- same name again; PG allows, shadowing the first
-- begin-expected
-- ok: 0
-- end-expected
RELEASE SAVEPOINT s;
-- releases the inner one
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK TO SAVEPOINT s;
-- PG: OK — the outer one is still there | mg: 3B001 does not exist;
