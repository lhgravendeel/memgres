-- source: investigation-2026-08.md
-- finding: 233
-- title: pg_locks is built by a nested loop over active sessions and tables plus the LOCK TABLE map and the advisory holds — it never asks the row-lock or table-lock man
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (id int);
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_locks WHERE relation = 'zz_p'::regclass;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_locks WHERE locktype='virtualxid' AND pid=pg_backend_pid();
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p VALUES (1);
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_locks WHERE locktype='transactionid' AND pid=pg_backend_pid();
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_p SET id = 2;
-- begin-expected
-- columns: mode:text
-- row: RowExclusiveLock
-- rowcount: 1
-- end-expected
SELECT mode FROM pg_locks WHERE relation='zz_p'::regclass AND pid=pg_backend_pid();
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- columns: id:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT id FROM zz_p WHERE id = 2 FOR UPDATE;
-- begin-expected
-- columns: mode:text
-- row: RowShareLock
-- rowcount: 1
-- end-expected
SELECT mode FROM pg_locks WHERE relation='zz_p'::regclass AND pid=pg_backend_pid();
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- columns: pg_typeof:regtype
-- row: integer[]
-- rowcount: 1
-- end-expected
SELECT pg_typeof(pg_blocking_pids(pg_backend_pid()));
