-- source: investigation-2026-08.md
-- finding: 207
-- title: Session and role state is changed outside the undo log. internalRollbackToSavepoint restores row locks, GUCs and pending notifications and nothing else; the ALT
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_b" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_b;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_c;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_b" does not exist
-- end-expected-error
GRANT zz_a TO zz_b;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_a" does not exist
-- end-expected-error
REASSIGN OWNED BY zz_a TO zz_b;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_roles WHERE rolname='zz_c';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
LISTEN zz_rb;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: count:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_listening_channels();
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SAVEPOINT sp;
-- begin-expected
-- columns: pg_advisory_xact_lock:void
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_advisory_xact_lock(9060002);
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK TO SAVEPOINT sp;
-- begin-expected
-- columns: count:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_locks WHERE locktype = 'advisory' AND objid = 9060002;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "zz_a" does not exist
-- end-expected-error
SET ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: current_user:name
-- row: memgres
-- rowcount: 1
-- end-expected
SELECT current_user;
