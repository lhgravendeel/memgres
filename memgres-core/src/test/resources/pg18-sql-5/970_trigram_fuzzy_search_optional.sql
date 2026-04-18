DROP SCHEMA IF EXISTS test_970 CASCADE;
CREATE SCHEMA test_970;
SET search_path TO test_970;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE names (
    name_id integer PRIMARY KEY,
    display_name text NOT NULL
);

INSERT INTO names VALUES
(1, 'postgres'),
(2, 'postgress'),
(3, 'postgre'),
(4, 'mysql');

-- begin-expected-error
-- message-like: invalid input syntax
-- end-expected-error
SELECT name_id
FROM names
WHERE display_name % 'postgres'
ORDER BY name_id;

-- begin-expected
-- columns: display_name|sim
-- row: postgres|1.0000000
-- row: postgress|0.7272727
-- row: postgre|0.7000000
-- row: mysql|0.0000000
-- end-expected
SELECT display_name,
       round(similarity(display_name, 'postgres')::numeric, 7) AS sim
FROM names
ORDER BY sim DESC, display_name;

-- begin-expected
-- columns: best_match
-- row: postgres
-- end-expected
SELECT display_name AS best_match
FROM names
ORDER BY similarity(display_name, 'postgres') DESC, display_name
LIMIT 1;
