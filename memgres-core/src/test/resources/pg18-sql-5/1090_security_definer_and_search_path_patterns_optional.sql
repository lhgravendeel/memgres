DROP SCHEMA IF EXISTS test_1090 CASCADE;
CREATE SCHEMA test_1090;
SET search_path TO test_1090;

CREATE TABLE secrets (
    secret_id integer PRIMARY KEY,
    secret_value text NOT NULL
);

INSERT INTO secrets VALUES
(1, 'top-secret');

CREATE FUNCTION get_secret_count()
RETURNS integer
LANGUAGE SQL
SECURITY DEFINER
SET search_path = test_1090
AS $$
  SELECT COUNT(*) FROM secrets
$$;

-- begin-expected
-- columns: secret_count
-- row: 1
-- end-expected
SELECT get_secret_count() AS secret_count;

-- begin-expected
-- columns: proc_exists
-- row: t
-- end-expected
SELECT to_regprocedure('test_1090.get_secret_count()') IS NOT NULL AS proc_exists;

