DROP SCHEMA IF EXISTS test_310 CASCADE;
CREATE SCHEMA test_310;
SET search_path TO test_310;

CREATE TABLE posts (
    post_id integer PRIMARY KEY,
    author_id integer NOT NULL,
    title text NOT NULL,
    created_at timestamp NOT NULL,
    score integer NOT NULL
);

INSERT INTO posts(post_id, author_id, title, created_at, score) VALUES
    (1, 10, 'alpha',   TIMESTAMP '2024-01-01 10:00:00', 5),
    (2, 10, 'beta',    TIMESTAMP '2024-01-01 10:00:00', 7),
    (3, 11, 'gamma',   TIMESTAMP '2024-01-01 09:00:00', 2),
    (4, 12, 'delta',   TIMESTAMP '2024-01-02 08:30:00', 8),
    (5, 11, 'epsilon', TIMESTAMP '2024-01-02 08:30:00', 4),
    (6, 12, 'zeta',    TIMESTAMP '2024-01-03 11:15:00', 6),
    (7, 10, 'eta',     TIMESTAMP '2024-01-03 11:15:00', 9),
    (8, 13, 'theta',   TIMESTAMP '2024-01-03 12:00:00', 3);

-- begin-expected
-- columns: post_id|title|created_at
-- row: 8|theta|2024-01-03 12:00:00
-- row: 7|eta|2024-01-03 11:15:00
-- row: 6|zeta|2024-01-03 11:15:00
-- end-expected
SELECT post_id, title, created_at
FROM posts
ORDER BY created_at DESC, post_id DESC
LIMIT 3;

-- begin-expected
-- columns: post_id|title|created_at
-- row: 5|epsilon|2024-01-02 08:30:00
-- row: 4|delta|2024-01-02 08:30:00
-- row: 2|beta|2024-01-01 10:00:00
-- end-expected
SELECT post_id, title, created_at
FROM posts
ORDER BY created_at DESC, post_id DESC
LIMIT 3 OFFSET 3;

-- begin-expected
-- columns: post_id|title|created_at
-- row: 5|epsilon|2024-01-02 08:30:00
-- row: 4|delta|2024-01-02 08:30:00
-- row: 2|beta|2024-01-01 10:00:00
-- end-expected
SELECT post_id, title, created_at
FROM posts
WHERE (created_at, post_id) < (TIMESTAMP '2024-01-03 11:15:00', 6)
ORDER BY created_at DESC, post_id DESC
LIMIT 3;

-- begin-expected
-- columns: author_id|post_id|title|created_at
-- row: 10|7|eta|2024-01-03 11:15:00
-- row: 11|5|epsilon|2024-01-02 08:30:00
-- row: 12|6|zeta|2024-01-03 11:15:00
-- row: 13|8|theta|2024-01-03 12:00:00
-- end-expected
SELECT DISTINCT ON (author_id) author_id, post_id, title, created_at
FROM posts
ORDER BY author_id, created_at DESC, post_id DESC;

-- begin-expected
-- columns: author_id|post_id|title|created_at
-- row: 10|7|eta|2024-01-03 11:15:00
-- row: 11|5|epsilon|2024-01-02 08:30:00
-- row: 12|6|zeta|2024-01-03 11:15:00
-- row: 13|8|theta|2024-01-03 12:00:00
-- end-expected
SELECT author_id, post_id, title, created_at
FROM (
    SELECT p.*, row_number() OVER (
        PARTITION BY author_id
        ORDER BY created_at DESC, post_id DESC
    ) AS rn
    FROM posts AS p
) AS ranked
WHERE rn = 1
ORDER BY author_id;

-- begin-expected
-- columns: post_id|title|page_no
-- row: 8|theta|1
-- row: 7|eta|1
-- row: 6|zeta|2
-- row: 5|epsilon|2
-- row: 4|delta|3
-- row: 2|beta|3
-- row: 1|alpha|4
-- row: 3|gamma|4
-- end-expected
SELECT post_id, title, ((row_number() OVER (ORDER BY created_at DESC, post_id DESC) - 1) / 2) + 1 AS page_no
FROM posts
ORDER BY created_at DESC, post_id DESC;

DROP SCHEMA test_310 CASCADE;
