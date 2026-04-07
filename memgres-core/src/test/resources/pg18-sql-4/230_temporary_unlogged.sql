DROP SCHEMA IF EXISTS test_230 CASCADE;
CREATE SCHEMA test_230;
SET search_path TO test_230;

CREATE TEMP TABLE temp_keep (
    id integer
) ON COMMIT DELETE ROWS;

BEGIN;
INSERT INTO temp_keep VALUES (1), (2);
COMMIT;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT COUNT(*) AS cnt
FROM temp_keep;

CREATE UNLOGGED TABLE fast_table (
    id integer PRIMARY KEY,
    note text
);

INSERT INTO fast_table VALUES
    (1, 'x'),
    (2, 'y');

-- begin-expected
-- columns: id|note
-- row: 1|x
-- row: 2|y
-- end-expected
SELECT id, note
FROM fast_table
ORDER BY id;

DROP SCHEMA test_230 CASCADE;
