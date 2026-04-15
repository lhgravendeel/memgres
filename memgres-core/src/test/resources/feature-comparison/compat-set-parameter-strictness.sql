-- ============================================================================
-- Feature Comparison: SET Parameter Strictness
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18: SET with an unrecognized parameter name throws error 42704
-- ("unrecognized configuration parameter"). Only registered GUC parameters
-- and custom-namespaced parameters (schema.param) are accepted.
--
-- Memgres: Silently accepts ANY parameter name, including typos and
-- completely fabricated names. This is intentional for pg_dump compatibility
-- but differs from PG behavior.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- 1. Unknown flat parameter should be rejected
-- ============================================================================

-- begin-expected-error
-- message-like: unrecognized configuration parameter
-- end-expected-error
SET completely_nonexistent_param = 'hello';

-- ============================================================================
-- 2. Misspelled parameter should be rejected
-- ============================================================================

-- begin-expected-error
-- message-like: unrecognized configuration parameter
-- end-expected-error
SET work_meme = '4MB';

-- ============================================================================
-- 3. Custom-namespaced parameter (with dot) should be accepted
-- ============================================================================

-- note: PG allows dotted custom parameters (custom_variable_classes).

-- command: SET
SET myapp.custom_setting = 'value';

-- begin-expected
-- columns: current_setting
-- row: value
-- end-expected
SELECT current_setting('myapp.custom_setting');

RESET myapp.custom_setting;

-- ============================================================================
-- 4. SHOW of unknown parameter should error
-- ============================================================================

-- begin-expected-error
-- message-like: unrecognized configuration parameter
-- end-expected-error
SHOW completely_nonexistent_param;

-- ============================================================================
-- 5. RESET of unknown parameter should error
-- ============================================================================

-- begin-expected-error
-- message-like: unrecognized configuration parameter
-- end-expected-error
RESET completely_nonexistent_param;

-- ============================================================================
-- 6. current_setting() with unknown parameter should error by default
-- ============================================================================

-- begin-expected-error
-- message-like: unrecognized configuration parameter
-- end-expected-error
SELECT current_setting('bogus_nonexistent_setting');

-- ============================================================================
-- 7. current_setting() with missing_ok=true should return NULL
-- ============================================================================

-- begin-expected
-- columns: current_setting
-- row:
-- end-expected
SELECT current_setting('bogus_nonexistent_setting', true);

-- ============================================================================
-- 8. Invalid boolean value for boolean parameter should error
-- ============================================================================

-- begin-expected-error
-- message-like: invalid value
-- end-expected-error
SET enable_seqscan = 'maybe';

-- ============================================================================
-- 9. Valid SET of known parameter should work
-- ============================================================================

-- command: SET
SET work_mem = '8MB';

-- begin-expected
-- columns: current_setting
-- row: 8MB
-- end-expected
SELECT current_setting('work_mem');

RESET work_mem;
