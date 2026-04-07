DROP SCHEMA IF EXISTS test_090 CASCADE;
CREATE SCHEMA test_090;
SET search_path TO test_090;

-- begin-expected
-- columns: sum_i|div_numeric|pow_val|mod_val|rounded
-- row: 7|2.5000000000000000|8.0000000000000000|1|3.14
-- end-expected
SELECT
    3 + 4 AS sum_i,
    5::numeric / 2 AS div_numeric,
    POWER(2::numeric, 3::numeric) AS pow_val,
    10 % 3 AS mod_val,
    ROUND(3.14159::numeric, 2) AS rounded;

-- begin-expected
-- columns: concat_val|len_val|sub_val|replaced|trimmed|fixed_eq
-- row: hello-world|5|bcd|fXrmat|hi|true
-- end-expected
SELECT
    'hello' || '-' || 'world' AS concat_val,
    LENGTH('hello') AS len_val,
    SUBSTRING('abcdef' FROM 2 FOR 3) AS sub_val,
    REPLACE('format', 'o', 'X') AS replaced,
    BTRIM('  hi  ') AS trimmed,
    ('A'::char(3) = 'A  '::char(3)) AS fixed_eq;

-- begin-expected
-- columns: bool1|bool2|bool3
-- row: true|true|true
-- end-expected
SELECT
    (true AND NOT false) AS bool1,
    (1 < 2 OR 2 < 1) AS bool2,
    ('abc' < 'abd') AS bool3;

DROP SCHEMA test_090 CASCADE;
