DROP SCHEMA IF EXISTS test_175 CASCADE;
CREATE SCHEMA test_175;
SET search_path TO test_175;

CREATE TEMP TABLE temp_numbers (n integer);
INSERT INTO temp_numbers(n) VALUES (1), (2), (3);

-- begin-expected
-- columns: current_path
-- row: test_175
-- end-expected
SELECT current_schema AS current_path;

SET application_name = 'pg_harness_demo';
-- begin-expected
-- columns: app_name
-- row: pg_harness_demo
-- end-expected
SELECT current_setting('application_name') AS app_name;

RESET application_name;
-- begin-expected
-- columns: app_name_after_reset
-- row: memgres
-- end-expected
SELECT current_setting('application_name', true) AS app_name_after_reset;

SET work_mem = '64kB';
-- begin-expected
-- columns: work_mem_before_local
-- row: 64kB
-- end-expected
SELECT current_setting('work_mem') AS work_mem_before_local;

BEGIN;
SET LOCAL work_mem = '128kB';
-- begin-expected
-- columns: work_mem_inside_tx
-- row: 128kB
-- end-expected
SELECT current_setting('work_mem') AS work_mem_inside_tx;
COMMIT;

-- begin-expected
-- columns: work_mem_after_tx
-- row: 64kB
-- end-expected
SELECT current_setting('work_mem') AS work_mem_after_tx;

SHOW search_path;

DISCARD TEMP;

-- begin-expected-error
-- message-like: relation "temp_numbers" does not exist
-- end-expected-error
SELECT * FROM temp_numbers;

RESET work_mem;
DROP SCHEMA test_175 CASCADE;
