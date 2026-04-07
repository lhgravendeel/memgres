-- Optional: requires postgres_fdw and suitable privileges
DROP SCHEMA IF EXISTS test_998 CASCADE;
CREATE SCHEMA test_998;
SET search_path TO test_998;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- begin-expected
-- columns: extension_present
-- row: f
-- end-expected
SELECT EXISTS (
  SELECT 1
  FROM pg_extension
  WHERE extname = 'postgres_fdw'
) AS extension_present;

