-- source: investigation-2026-08.md
-- finding: 231
-- title: A waiter steps over a doomed transaction's row lock (awaitConcurrentWrite returns on blocker.isDoomed(), lockRowWaiting deletes a doomed session's lock entries)
-- A: BEGIN; UPDATE zz_dl SET v=100 WHERE i=1;  B: BEGIN; UPDATE zz_dl SET v=200 WHERE i=2;
-- A: UPDATE zz_dl SET v=300 WHERE i=2;  (waits)   B: UPDATE zz_dl SET v=400 WHERE i=1;
-- A: COMMIT;                                     B: ROLLBACK;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_dl" does not exist
-- end-expected-error
SELECT i, v FROM zz_dl ORDER BY i;
-- begin-expected
-- columns: pg_advisory_lock:void
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_advisory_lock(-1);
-- begin-expected
-- columns: classid:text | objid:text
-- row: 4294967295 | 4294967295
-- rowcount: 1
-- end-expected
SELECT classid::text, objid::text FROM pg_locks WHERE locktype = 'advisory';
