DROP SCHEMA IF EXISTS test_250 CASCADE;
CREATE SCHEMA test_250;
SET search_path TO test_250;

CREATE TABLE words (
    w text
);

INSERT INTO words VALUES
    ('alpha'),
    ('Alphabet'),
    ('beta'),
    ('gamma-123');

-- begin-expected
-- columns: w
-- row: alpha
-- end-expected
SELECT w
FROM words
WHERE w LIKE 'a%'
  AND w NOT LIKE '%A%'
ORDER BY w;

-- begin-expected
-- columns: w
-- row: Alphabet
-- row: alpha
-- end-expected
SELECT w
FROM words
WHERE w ILIKE 'a%'
ORDER BY w;

-- begin-expected
-- columns: w
-- row: gamma-123
-- end-expected
SELECT w
FROM words
WHERE w SIMILAR TO '(gamma|delta)-[0-9]+';

-- begin-expected
-- columns: w
-- row: gamma-123
-- end-expected
SELECT w
FROM words
WHERE w ~ '^[a-z]+-[0-9]+$';

DROP SCHEMA test_250 CASCADE;
