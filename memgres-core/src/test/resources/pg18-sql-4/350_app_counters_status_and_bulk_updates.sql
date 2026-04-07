DROP SCHEMA IF EXISTS test_350 CASCADE;
CREATE SCHEMA test_350;
SET search_path TO test_350;

CREATE TABLE threads (
    thread_id integer PRIMARY KEY,
    subject text NOT NULL,
    comment_count integer NOT NULL DEFAULT 0
);

CREATE TABLE comments (
    comment_id integer PRIMARY KEY,
    thread_id integer NOT NULL REFERENCES threads(thread_id),
    state text NOT NULL,
    created_at timestamp NOT NULL,
    read_at timestamp
);

INSERT INTO threads(thread_id, subject) VALUES
    (1, 'release'),
    (2, 'roadmap'),
    (3, 'support');

INSERT INTO comments(comment_id, thread_id, state, created_at, read_at) VALUES
    (1, 1, 'published', TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 11:00:00'),
    (2, 1, 'published', TIMESTAMP '2024-01-02 10:00:00', NULL),
    (3, 2, 'draft',     TIMESTAMP '2024-01-03 09:00:00', NULL),
    (4, 2, 'published', TIMESTAMP '2024-01-04 09:00:00', NULL),
    (5, 3, 'published', TIMESTAMP '2024-01-05 08:00:00', TIMESTAMP '2024-01-05 08:30:00');

UPDATE threads AS t
SET comment_count = src.cnt
FROM (
    SELECT thread_id, count(*)::integer AS cnt
    FROM comments
    WHERE state = 'published'
    GROUP BY thread_id
) AS src
WHERE src.thread_id = t.thread_id;

-- begin-expected
-- columns: thread_id|subject|comment_count
-- row: 1|release|2
-- row: 2|roadmap|1
-- row: 3|support|1
-- end-expected
SELECT thread_id, subject, comment_count
FROM threads
ORDER BY thread_id;

-- begin-expected
-- columns: state|cnt
-- row: draft|1
-- row: published|4
-- end-expected
SELECT state, count(*) AS cnt
FROM comments
GROUP BY state
ORDER BY state;

-- begin-expected
-- columns: thread_id|unread_count
-- row: 1|1
-- row: 2|2
-- row: 3|0
-- end-expected
SELECT thread_id,
       count(*) FILTER (WHERE read_at IS NULL) AS unread_count
FROM comments
GROUP BY thread_id
ORDER BY thread_id;

UPDATE comments
SET read_at = TIMESTAMP '2024-01-10 00:00:00'
WHERE thread_id = 2
  AND read_at IS NULL;

-- begin-expected
-- columns: thread_id|unread_count
-- row: 1|1
-- row: 2|0
-- row: 3|0
-- end-expected
SELECT thread_id,
       count(*) FILTER (WHERE read_at IS NULL) AS unread_count
FROM comments
GROUP BY thread_id
ORDER BY thread_id;

DELETE FROM comments
WHERE state = 'draft';

-- begin-expected
-- columns: comment_id|thread_id|state
-- row: 1|1|published
-- row: 2|1|published
-- row: 4|2|published
-- row: 5|3|published
-- end-expected
SELECT comment_id, thread_id, state
FROM comments
ORDER BY comment_id;

DROP SCHEMA test_350 CASCADE;
