-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: LISTEN/NOTIFY, advisory locks and DISCARD
-- title: Unrelated singletons
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
