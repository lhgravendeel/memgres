DROP SCHEMA IF EXISTS test_950 CASCADE;
CREATE SCHEMA test_950;
SET search_path TO test_950;

CREATE TABLE mix (
    id integer PRIMARY KEY,
    t text,
    n numeric(10,2),
    b boolean,
    j jsonb,
    a integer[]
);

INSERT INTO mix VALUES
(1, 'alpha', 10.50, true, '{"k":"v"}', ARRAY[1,2]),
(2, 'beta',  NULL, false, '{"k":"w"}', ARRAY[3]);

-- begin-expected
-- columns: id,t,n,b,j,a
-- row: 1|alpha|10.50|t|{"k": "v"}|{1,2}
-- row: 2|beta||f|{"k": "w"}|{3}
-- end-expected
SELECT id, t, n, b, j, a
FROM mix
ORDER BY id;

-- begin-expected
-- columns: id,dup,dup
-- row: 1|alpha|alpha
-- row: 2|beta|beta
-- end-expected
SELECT id, t AS dup, t AS dup
FROM mix
ORDER BY id;

-- begin-expected
-- columns: id
-- end-expected
SELECT id
FROM mix
WHERE false
ORDER BY id;

-- begin-expected
-- columns: answer,description
-- row: 42|wide row test
-- end-expected
SELECT 42 AS answer, 'wide row test' AS description;

