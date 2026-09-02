-- source: investigation-2026-08.md
-- finding: 240
-- title: Unrelated singletons in this area
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
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized VACUUM option "bogus_option"
-- end-expected-error
VACUUM (BOGUS_OPTION);
-- begin-expected-error
-- sqlstate: 42P18
-- message-like: cannot determine type of empty array
-- end-expected-error
SELECT ARRAY[];
-- begin-expected-error
-- sqlstate: 42601
-- message-like: zero-length delimited identifier at or near """"
-- end-expected-error
LISTEN "";
-- begin-expected-error
-- sqlstate: 42601
-- message-like: zero-length delimited identifier at or near """"
-- end-expected-error
NOTIFY "";
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'nokeyword'"
-- end-expected-error
NOTIFY zz_ex 'nokeyword';
-- begin-expected
-- columns: pg_advisory_unlock:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_advisory_unlock(9060777);
-- begin-expected
-- columns: pg_advisory_unlock_shared:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_advisory_unlock_shared(9060777);
-- begin-expected
-- columns: pg_advisory_lock:void
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_advisory_lock(9060778);
-- begin-expected
-- columns: pg_advisory_unlock_shared:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_advisory_unlock_shared(9060778);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized EXPLAIN option "bogus_option"
-- end-expected-error
EXPLAIN (BOGUS_OPTION) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: analyze requires a Boolean value
-- end-expected-error
EXPLAIN (ANALYZE notaboolean) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: payload string too long
-- end-expected-error
DO $$ BEGIN EXECUTE 'NOTIFY zz_b, ' || quote_literal(repeat(U&'\00E9', 4000)); END $$;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: payload string too long
-- end-expected-error
DO $$ BEGIN EXECUTE 'NOTIFY zz_b, ' || quote_literal(repeat(U&'\00E9', 5000)); END $$;
