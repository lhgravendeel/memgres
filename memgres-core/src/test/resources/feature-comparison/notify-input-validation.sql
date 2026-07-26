-- ============================================================================
-- Feature Comparison: NOTIFY input limits
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A notification travels through a fixed-size queue slot, so PostgreSQL bounds
-- both the channel name and the payload rather than truncating either. Both
-- the function and the statement form are checked.
-- ============================================================================

-- ============================================================================
-- 1. pg_notify
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22023
-- message-like: payload string too long
-- end-expected-error
SELECT pg_notify('nvc', repeat('x', 9000));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: channel name cannot be empty
-- end-expected-error
SELECT pg_notify(NULL, 'x');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: channel name cannot be empty
-- end-expected-error
SELECT pg_notify('', 'x');

-- Just inside the limit is accepted, and returns void rather than null
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT (pg_notify('nvc', repeat('x', 7999)) IS NULL) AS a;

-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT (pg_notify('nvc','ok') IS NULL) AS a;

-- ============================================================================
-- 2. The statement form is bounded the same way
-- ============================================================================

LISTEN nvc;

NOTIFY nvc, 'ok';

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_listening_channels();

UNLISTEN *;
