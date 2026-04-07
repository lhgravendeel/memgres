DROP SCHEMA IF EXISTS test_1270 CASCADE;
CREATE SCHEMA test_1270;
SET search_path TO test_1270;

CREATE TABLE posts (
    post_id integer PRIMARY KEY,
    title text NOT NULL,
    comment_count integer NOT NULL DEFAULT 0,
    last_comment_at timestamp
);

CREATE TABLE comments (
    comment_id integer PRIMARY KEY,
    post_id integer NOT NULL,
    created_at timestamp NOT NULL
);

INSERT INTO posts VALUES
(1, 'one', 0, NULL),
(2, 'two', 0, NULL);

INSERT INTO comments VALUES
(10, 1, '2024-01-01 10:00'),
(11, 1, '2024-01-01 11:00'),
(20, 2, '2024-01-02 09:00');

UPDATE posts p
SET comment_count = s.cnt,
    last_comment_at = s.max_created_at
FROM (
    SELECT post_id, COUNT(*) AS cnt, MAX(created_at) AS max_created_at
    FROM comments
    GROUP BY post_id
) s
WHERE p.post_id = s.post_id;

-- begin-expected
-- columns: post_id,comment_count,last_comment_at
-- row: 1|2|2024-01-01 11:00:00
-- row: 2|1|2024-01-02 09:00:00
-- end-expected
SELECT post_id, comment_count, last_comment_at
FROM posts
ORDER BY post_id;

-- begin-expected
-- columns: stale_rows
-- row: 0
-- end-expected
SELECT COUNT(*) AS stale_rows
FROM posts p
JOIN (
    SELECT post_id, COUNT(*) AS cnt, MAX(created_at) AS max_created_at
    FROM comments
    GROUP BY post_id
) s USING (post_id)
WHERE p.comment_count <> s.cnt
   OR p.last_comment_at <> s.max_created_at;

