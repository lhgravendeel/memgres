DROP SCHEMA IF EXISTS test_890 CASCADE;
CREATE SCHEMA test_890;
SET search_path TO test_890;

CREATE TABLE posts (
    post_id integer PRIMARY KEY,
    title text NOT NULL
);

CREATE TABLE comments (
    comment_id integer PRIMARY KEY,
    post_id integer NOT NULL,
    body text NOT NULL,
    created_at timestamp NOT NULL
);

INSERT INTO posts VALUES
(1, 'one'),
(2, 'two');

INSERT INTO comments VALUES
(10, 1, 'a', '2024-01-01 10:00'),
(11, 1, 'b', '2024-01-01 11:00'),
(12, 1, 'c', '2024-01-01 12:00'),
(20, 2, 'x', '2024-01-02 09:00'),
(21, 2, 'y', '2024-01-02 10:00');

-- begin-expected
-- columns: post_id,comment_id,body
-- row: 1|12|c
-- row: 1|11|b
-- row: 2|21|y
-- row: 2|20|x
-- end-expected
SELECT p.post_id, c.comment_id, c.body
FROM posts p
CROSS JOIN LATERAL (
    SELECT comment_id, body
    FROM comments c
    WHERE c.post_id = p.post_id
    ORDER BY c.created_at DESC
    LIMIT 2
) c
ORDER BY p.post_id, c.comment_id DESC;

-- begin-expected
-- columns: post_id,latest_comment_id
-- row: 1|12
-- row: 2|21
-- end-expected
SELECT p.post_id, c.comment_id AS latest_comment_id
FROM posts p
LEFT JOIN LATERAL (
    SELECT comment_id
    FROM comments c
    WHERE c.post_id = p.post_id
    ORDER BY c.created_at DESC
    LIMIT 1
) c ON true
ORDER BY p.post_id;

-- begin-expected
-- columns: post_id,comment_count
-- row: 1|3
-- row: 2|2
-- end-expected
SELECT p.post_id, x.comment_count
FROM posts p
JOIN LATERAL (
    SELECT COUNT(*) AS comment_count
    FROM comments c
    WHERE c.post_id = p.post_id
) x ON true
ORDER BY p.post_id;

