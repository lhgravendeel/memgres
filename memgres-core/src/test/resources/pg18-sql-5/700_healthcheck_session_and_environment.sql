DROP SCHEMA IF EXISTS test_700 CASCADE;
CREATE SCHEMA test_700;
SET search_path TO test_700;

-- begin-expected
-- columns: ok
-- row: 1
-- end-expected
SELECT 1 AS ok;

-- begin-expected
-- columns: current_schema
-- row: test_700
-- end-expected
SELECT current_schema();

-- begin-expected
-- columns: current_database
-- row: test
-- end-expected
SELECT current_database();

-- begin-expected
-- columns: search_path_value
-- row: test_700
-- end-expected
SELECT current_setting('search_path') AS search_path_value;

-- begin-expected
-- columns: user_eq_session_user
-- row: t
-- end-expected
SELECT current_user = session_user AS user_eq_session_user;

