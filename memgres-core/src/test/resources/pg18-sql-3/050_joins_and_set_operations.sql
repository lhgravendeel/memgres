DROP SCHEMA IF EXISTS test_050 CASCADE;
CREATE SCHEMA test_050;
SET search_path TO test_050;

CREATE TABLE authors (
    author_id integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE books (
    book_id integer PRIMARY KEY,
    author_id integer,
    title text NOT NULL
);

INSERT INTO authors VALUES
    (1, 'Ada'),
    (2, 'Ben'),
    (3, 'Cy');

INSERT INTO books VALUES
    (10, 1, 'A-1'),
    (11, 1, 'A-2'),
    (12, 2, 'B-1'),
    (13, NULL, 'Orphan');

-- begin-expected
-- columns: name|title
-- row: Ada|A-1
-- row: Ada|A-2
-- row: Ben|B-1
-- end-expected
SELECT a.name, b.title
FROM authors a
JOIN books b ON a.author_id = b.author_id
ORDER BY a.name, b.title;

-- begin-expected
-- columns: name|title
-- row: Ada|A-1
-- row: Ada|A-2
-- row: Ben|B-1
-- row: Cy|
-- end-expected
SELECT a.name, b.title
FROM authors a
LEFT JOIN books b ON a.author_id = b.author_id
ORDER BY a.name, b.title NULLS LAST;

-- begin-expected
-- columns: name|title
-- row: Ada|A-1
-- row: Ada|A-2
-- row: Ben|B-1
-- row: |Orphan
-- end-expected
SELECT a.name, b.title
FROM authors a
RIGHT JOIN books b ON a.author_id = b.author_id
ORDER BY a.name NULLS LAST, b.title;

-- begin-expected
-- columns: left_name|right_name
-- row: Ada|Ben
-- row: Ada|Cy
-- row: Ben|Cy
-- end-expected
SELECT a1.name AS left_name, a2.name AS right_name
FROM authors a1
CROSS JOIN authors a2
WHERE a1.author_id < a2.author_id
ORDER BY 1, 2;

-- begin-expected
-- columns: val
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT val
FROM (
    SELECT 1 AS val
    UNION
    SELECT 2
    UNION
    SELECT 2
    UNION
    SELECT 3
) s
ORDER BY val;

-- begin-expected
-- columns: val
-- row: 1
-- row: 2
-- row: 2
-- row: 3
-- end-expected
SELECT val
FROM (
    SELECT 1 AS val
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3
) s
ORDER BY val;

-- begin-expected
-- columns: val
-- row: 1
-- row: 2
-- end-expected
SELECT val
FROM (
    SELECT 1 AS val
    UNION
    SELECT 2
    INTERSECT
    SELECT 2
) s
ORDER BY val;

-- begin-expected
-- columns: val
-- row: 1
-- row: 3
-- end-expected
SELECT val
FROM (
    SELECT 1 AS val
    UNION
    SELECT 2
    UNION
    SELECT 3
    EXCEPT
    SELECT 2
) s
ORDER BY val;

DROP SCHEMA test_050 CASCADE;
