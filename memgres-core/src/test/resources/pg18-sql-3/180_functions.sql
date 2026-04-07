DROP SCHEMA IF EXISTS test_180 CASCADE;
CREATE SCHEMA test_180;
SET search_path TO test_180;

CREATE TABLE nums (
    n integer PRIMARY KEY
);

INSERT INTO nums VALUES (1), (2), (3), (4);

CREATE FUNCTION add_one(x integer)
RETURNS integer
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT x + 1
$$;

CREATE FUNCTION classify_num(x integer)
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    IF x % 2 = 0 THEN
        RETURN 'even';
    ELSE
        RETURN 'odd';
    END IF;
END;
$$;

CREATE FUNCTION range_table(max_n integer)
RETURNS TABLE(val integer, square integer)
LANGUAGE SQL
AS $$
    SELECT g, g * g
    FROM generate_series(1, max_n) AS g
$$;

CREATE FUNCTION with_defaults(a integer, b integer DEFAULT 5)
RETURNS integer
LANGUAGE SQL
AS $$
    SELECT a + b
$$;

-- begin-expected
-- columns: n|plus_one|class
-- row: 1|2|odd
-- row: 2|3|even
-- row: 3|4|odd
-- row: 4|5|even
-- end-expected
SELECT n, add_one(n) AS plus_one, classify_num(n) AS class
FROM nums
ORDER BY n;

-- begin-expected
-- columns: val|square
-- row: 1|1
-- row: 2|4
-- row: 3|9
-- end-expected
SELECT val, square
FROM range_table(3)
ORDER BY val;

-- begin-expected
-- columns: v1|v2
-- row: 8|9
-- end-expected
SELECT with_defaults(3) AS v1, with_defaults(a => 3, b => 6) AS v2;

DROP SCHEMA test_180 CASCADE;
