DROP SCHEMA IF EXISTS test_1220 CASCADE;
CREATE SCHEMA test_1220;
SET search_path TO test_1220;

CREATE TYPE person_name AS (
    first_name text,
    last_name text
);

CREATE TABLE people (
    person_id integer PRIMARY KEY,
    name person_name NOT NULL,
    age integer NOT NULL
);

INSERT INTO people VALUES
(1, ('Alice','Zeal'), 30),
(2, ('Bob','Yellow'), 25),
(3, ('Cara','Yellow'), 25);

CREATE FUNCTION full_name(p person_name)
RETURNS text
LANGUAGE SQL
AS $$
  SELECT p.first_name || ' ' || p.last_name
$$;

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT person_id, full_name(name) AS rendered_name
FROM people
ORDER BY person_id;

-- begin-expected
-- columns: person_id,is_lt
-- row: 1|f
-- row: 2|t
-- row: 3|t
-- end-expected
SELECT person_id,
       ROW(age, (name).last_name) < ROW(30, 'Zeal') AS is_lt
FROM people
ORDER BY person_id;

-- begin-expected
-- columns: person_id,is_distinct
-- row: 1|f
-- row: 2|t
-- row: 3|t
-- end-expected
SELECT person_id,
       ROW((name).first_name, age) IS DISTINCT FROM ROW('Alice', 30) AS is_distinct
FROM people
ORDER BY person_id;

-- begin-expected
-- columns: first_name,last_name
-- row: Alice|Zeal
-- row: Bob|Yellow
-- row: Cara|Yellow
-- end-expected
SELECT (name).first_name AS first_name, (name).last_name AS last_name
FROM people
ORDER BY first_name;

