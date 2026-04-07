-- Optional: large object APIs
DROP SCHEMA IF EXISTS test_999 CASCADE;
CREATE SCHEMA test_999;
SET search_path TO test_999;

-- begin-expected
-- columns: lo_oid_is_positive
-- row: t
-- end-expected
SELECT lo_create(0) > 0 AS lo_oid_is_positive;

