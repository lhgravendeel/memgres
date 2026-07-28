-- ============================================================================
-- Feature Comparison: statement_timeout (single-connection)
-- A statement that runs past statement_timeout is cancelled with SQLSTATE 57014
-- and PG's wording, the transaction it was in is left aborted, and the session
-- recovers on ROLLBACK. Also covers the spellings SET accepts, 0 meaning no
-- limit, and statements that finish inside the limit being untouched.
-- lock_timeout and idle_in_transaction_session_timeout need a second session or
-- a wall-clock wait; only their settings are observable here, the behaviour is
-- covered by StatementTimeoutTest.
-- The runaway statements use a series far larger than either engine can walk in
-- 50ms, so the cancellation is what ends them, not completion.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Spellings SET accepts, and how SHOW reports them back
-- ---------------------------------------------------------------------------

-- 1. Default is 0 (no limit)
-- begin-expected
-- columns: statement_timeout
-- row: 0
-- end-expected
SHOW statement_timeout;

-- 2. Seconds
SET statement_timeout = '2s';

-- begin-expected
-- columns: statement_timeout
-- row: 2s
-- end-expected
SHOW statement_timeout;

-- 3. A bare integer is milliseconds, and SHOW canonicalises the unit
SET statement_timeout = 2000;

-- begin-expected
-- columns: statement_timeout
-- row: 2s
-- end-expected
SHOW statement_timeout;

-- 4. Milliseconds, spelled and bare
SET statement_timeout = '500ms';

-- begin-expected
-- columns: statement_timeout
-- row: 500ms
-- end-expected
SHOW statement_timeout;

SET statement_timeout = 500;

-- begin-expected
-- columns: statement_timeout
-- row: 500ms
-- end-expected
SHOW statement_timeout;

-- 5. Minutes and hours
SET statement_timeout = '1min';

-- begin-expected
-- columns: statement_timeout
-- row: 1min
-- end-expected
SHOW statement_timeout;

SET statement_timeout = '1h';

-- begin-expected
-- columns: statement_timeout
-- row: 1h
-- end-expected
SHOW statement_timeout;

-- 6. SET SESSION is the same as plain SET
SET SESSION statement_timeout = '3s';

-- begin-expected
-- columns: statement_timeout
-- row: 3s
-- end-expected
SHOW statement_timeout;

-- 7. set_config() returns the value it installed
-- begin-expected
-- columns: set_config
-- row: 750ms
-- end-expected
SELECT set_config('statement_timeout', '750ms', false);

-- begin-expected
-- columns: statement_timeout
-- row: 750ms
-- end-expected
SHOW statement_timeout;

-- 8. current_setting() reads the same value
SET statement_timeout = '250ms';

-- begin-expected
-- columns: current_setting
-- row: 250ms
-- end-expected
SELECT current_setting('statement_timeout');

-- 9. Zero, in both spellings, means no limit
SET statement_timeout = 0;

-- begin-expected
-- columns: statement_timeout
-- row: 0
-- end-expected
SHOW statement_timeout;

SET statement_timeout = '0';

-- begin-expected
-- columns: statement_timeout
-- row: 0
-- end-expected
SHOW statement_timeout;

-- 10. TO DEFAULT and RESET both go back to no limit
SET statement_timeout = '5s';
SET statement_timeout TO DEFAULT;

-- begin-expected
-- columns: statement_timeout
-- row: 0
-- end-expected
SHOW statement_timeout;

SET statement_timeout = '5s';
RESET statement_timeout;

-- begin-expected
-- columns: statement_timeout
-- row: 0
-- end-expected
SHOW statement_timeout;

-- 11. pg_settings reports it as an integer millisecond setting
-- begin-expected
-- columns: name, setting, unit
-- row: statement_timeout, 0, ms
-- end-expected
SELECT name, setting, unit FROM pg_settings WHERE name = 'statement_timeout';

-- ---------------------------------------------------------------------------
-- The timeout fires
-- ---------------------------------------------------------------------------

-- 12. A runaway SELECT is cancelled: 57014 canceling statement due to statement timeout
SET statement_timeout = '50ms';
SELECT count(*) FROM generate_series(1, 200000000);

-- 13. The session is usable straight away afterwards (autocommit, nothing to roll back)
-- begin-expected
-- columns: after_select
-- row: 1
-- end-expected
SELECT 1 AS after_select;

-- 14. A row filter does not save it either
SELECT count(*) FROM generate_series(1, 200000000) g WHERE g % 7 = 0;

-- 15. A runaway recursive CTE is cancelled the same way
WITH RECURSIVE sto_r(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM sto_r WHERE n < 200000000
)
SELECT count(*) FROM sto_r;

-- 16. The limit is per statement, not cumulative: a quick one still succeeds
-- begin-expected
-- columns: quick
-- row: 100
-- end-expected
SELECT count(*) AS quick FROM generate_series(1, 100);

SET statement_timeout = 0;

-- ---------------------------------------------------------------------------
-- A cancelled write leaves nothing behind
-- ---------------------------------------------------------------------------

CREATE TABLE sto_dml (n int);

-- 17. INSERT ... SELECT is cancelled
SET statement_timeout = '50ms';
INSERT INTO sto_dml SELECT g FROM generate_series(1, 200000000) g;

SET statement_timeout = 0;

-- 18. No rows were left behind
-- begin-expected
-- columns: rows_left
-- row: 0
-- end-expected
SELECT count(*) AS rows_left FROM sto_dml;

DROP TABLE sto_dml;

-- 19. CREATE TABLE AS is cancelled, and the table is not created
SET statement_timeout = '50ms';
CREATE TABLE sto_ctas AS SELECT g FROM generate_series(1, 200000000) g;

SET statement_timeout = 0;
SELECT count(*) FROM sto_ctas;

-- ---------------------------------------------------------------------------
-- Inside a transaction block
-- ---------------------------------------------------------------------------

SET statement_timeout = '50ms';
BEGIN;

-- 20. The statement is cancelled...
SELECT count(*) FROM generate_series(1, 200000000);

-- 21. ...and the transaction is left aborted: 25P02 until it ends
SELECT 1;

ROLLBACK;

SET statement_timeout = 0;

-- 22. The session is healthy again
-- begin-expected
-- columns: recovered
-- row: 1
-- end-expected
SELECT 1 AS recovered;

-- ---------------------------------------------------------------------------
-- SET LOCAL scopes the limit to the transaction
-- ---------------------------------------------------------------------------

BEGIN;
SET LOCAL statement_timeout = '50ms';

-- begin-expected
-- columns: statement_timeout
-- row: 50ms
-- end-expected
SHOW statement_timeout;

-- 23. In force inside the transaction
SELECT count(*) FROM generate_series(1, 200000000);

ROLLBACK;

-- 24. Gone again outside it
-- begin-expected
-- columns: statement_timeout
-- row: 0
-- end-expected
SHOW statement_timeout;

-- ---------------------------------------------------------------------------
-- What the timeout must NOT affect
-- ---------------------------------------------------------------------------

-- 25. With no limit set, a long-ish query runs to completion
SET statement_timeout = 0;

-- begin-expected
-- columns: unlimited
-- row: 20000
-- end-expected
SELECT count(*) AS unlimited FROM generate_series(1, 20000);

-- 26. A statement that finishes inside a generous limit is untouched
SET statement_timeout = '30s';

-- begin-expected
-- columns: total
-- row: 2001000
-- end-expected
SELECT sum(g) AS total FROM generate_series(1, 2000) g;

-- begin-expected
-- columns: depth
-- row: 500
-- end-expected
WITH RECURSIVE sto_u(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM sto_u WHERE n < 500
)
SELECT count(*) AS depth FROM sto_u;

-- begin-expected
-- columns: r
-- row: abababababababababab
-- end-expected
SELECT repeat('ab', 10) AS r;

-- 27. Transaction control is never limited
SET statement_timeout = '50ms';
BEGIN;
SAVEPOINT sto_sp;
RELEASE SAVEPOINT sto_sp;
COMMIT;
BEGIN;
ROLLBACK;
SET statement_timeout = 0;

-- ---------------------------------------------------------------------------
-- The neighbouring timeout settings
-- ---------------------------------------------------------------------------

-- 28. lock_timeout takes the same spellings
-- begin-expected
-- columns: lock_timeout
-- row: 0
-- end-expected
SHOW lock_timeout;

SET lock_timeout = '2s';

-- begin-expected
-- columns: lock_timeout
-- row: 2s
-- end-expected
SHOW lock_timeout;

SET lock_timeout = 1500;

-- begin-expected
-- columns: lock_timeout
-- row: 1500ms
-- end-expected
SHOW lock_timeout;

SET lock_timeout = 0;

-- begin-expected
-- columns: lock_timeout
-- row: 0
-- end-expected
SHOW lock_timeout;

-- begin-expected
-- columns: name, setting, unit
-- row: lock_timeout, 0, ms
-- end-expected
SELECT name, setting, unit FROM pg_settings WHERE name = 'lock_timeout';

-- 29. idle_in_transaction_session_timeout likewise
-- begin-expected
-- columns: idle_in_transaction_session_timeout
-- row: 0
-- end-expected
SHOW idle_in_transaction_session_timeout;

SET idle_in_transaction_session_timeout = '5s';

-- begin-expected
-- columns: idle_in_transaction_session_timeout
-- row: 5s
-- end-expected
SHOW idle_in_transaction_session_timeout;

SET idle_in_transaction_session_timeout = 0;

-- begin-expected
-- columns: idle_in_transaction_session_timeout
-- row: 0
-- end-expected
SHOW idle_in_transaction_session_timeout;
