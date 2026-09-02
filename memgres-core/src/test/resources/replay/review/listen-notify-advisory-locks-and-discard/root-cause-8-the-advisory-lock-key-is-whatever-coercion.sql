-- source: review-2026-08.md
-- finding: Root cause 8: the advisory-lock key is whatever coercion produces, not a typed argument
-- area: LISTEN/NOTIFY, advisory locks and DISCARD
-- title: Root cause 8: the advisory-lock key is whatever coercion produces, not a typed argument
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_try_advisory_lock(bigint, integer) does not exist
-- end-expected-error
SELECT pg_try_advisory_lock(4294967296, 5);
-- begin-expected
-- columns: pg_try_advisory_lock:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_try_advisory_lock(0, 5);
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_try_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_try_advisory_lock(9223372036854775808);
-- begin-expected
-- columns: pg_advisory_lock:void
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_advisory_lock(-1);
-- begin-expected
-- columns: classid:text | objid:text
-- row: 0 | 5
-- row: 4294967295 | 4294967295
-- rowcount: 2
-- end-expected
SELECT classid::text, objid::text FROM pg_locks WHERE locktype = 'advisory';
