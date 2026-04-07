DROP SCHEMA IF EXISTS test_000 CASCADE;
CREATE SCHEMA test_000;
SET search_path TO test_000;

CREATE TABLE smoke (
    id integer PRIMARY KEY,
    name text NOT NULL
);

INSERT INTO smoke(id, name) VALUES
    (1, 'alpha'),
    (2, 'beta'),
    (3, 'gamma');

-- begin-expected
-- columns: one
-- row: 1
-- end-expected
SELECT 1 AS one;

-- begin-expected
-- columns: id|name
-- row: 1|alpha
-- row: 2|beta
-- row: 3|gamma
-- end-expected
SELECT id, name
FROM smoke
ORDER BY id;

-- begin-expected
-- columns: id|name
-- end-expected
SELECT id, name
FROM smoke
WHERE id > 100
ORDER BY id;

DROP SCHEMA test_000 CASCADE;
