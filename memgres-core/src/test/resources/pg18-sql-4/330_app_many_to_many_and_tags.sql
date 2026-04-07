DROP SCHEMA IF EXISTS test_330 CASCADE;
CREATE SCHEMA test_330;
SET search_path TO test_330;

CREATE TABLE posts (
    post_id integer PRIMARY KEY,
    title text NOT NULL
);

CREATE TABLE tags (
    tag_id integer PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE post_tags (
    post_id integer NOT NULL REFERENCES posts(post_id),
    tag_id integer NOT NULL REFERENCES tags(tag_id),
    PRIMARY KEY (post_id, tag_id)
);

INSERT INTO posts(post_id, title) VALUES
    (1, 'orm tips'),
    (2, 'pg indexing'),
    (3, 'release notes'),
    (4, 'deploy guide');

INSERT INTO tags(tag_id, name) VALUES
    (1, 'postgres'),
    (2, 'performance'),
    (3, 'release'),
    (4, 'ops');

INSERT INTO post_tags(post_id, tag_id) VALUES
    (1, 1),
    (2, 1),
    (2, 2),
    (3, 1),
    (3, 3),
    (4, 2),
    (4, 4);

-- begin-expected
-- columns: post_id|title|tag_name
-- row: 1|orm tips|postgres
-- row: 2|pg indexing|performance
-- row: 2|pg indexing|postgres
-- row: 3|release notes|postgres
-- row: 3|release notes|release
-- row: 4|deploy guide|ops
-- row: 4|deploy guide|performance
-- end-expected
SELECT p.post_id, p.title, t.name AS tag_name
FROM posts AS p
JOIN post_tags AS pt ON pt.post_id = p.post_id
JOIN tags AS t ON t.tag_id = pt.tag_id
ORDER BY p.post_id, t.name;

-- begin-expected
-- columns: post_id|title
-- row: 1|orm tips
-- row: 2|pg indexing
-- row: 3|release notes
-- end-expected
SELECT DISTINCT p.post_id, p.title
FROM posts AS p
JOIN post_tags AS pt ON pt.post_id = p.post_id
JOIN tags AS t ON t.tag_id = pt.tag_id
WHERE t.name IN ('postgres', 'release')
ORDER BY p.post_id;

-- begin-expected
-- columns: post_id|title
-- row: 2|pg indexing
-- end-expected
SELECT p.post_id, p.title
FROM posts AS p
JOIN post_tags AS pt ON pt.post_id = p.post_id
JOIN tags AS t ON t.tag_id = pt.tag_id
WHERE t.name IN ('postgres', 'performance')
GROUP BY p.post_id, p.title
HAVING count(DISTINCT t.name) = 2
ORDER BY p.post_id;

-- begin-expected
-- columns: post_id|title
-- row: 4|deploy guide
-- end-expected
SELECT p.post_id, p.title
FROM posts AS p
WHERE NOT EXISTS (
    SELECT 1
    FROM post_tags AS pt
    JOIN tags AS t ON t.tag_id = pt.tag_id
    WHERE pt.post_id = p.post_id
      AND t.name = 'postgres'
)
ORDER BY p.post_id;

-- begin-expected
-- columns: tag_name|post_count
-- row: ops|1
-- row: performance|2
-- row: postgres|3
-- row: release|1
-- end-expected
SELECT t.name AS tag_name, count(pt.post_id) AS post_count
FROM tags AS t
LEFT JOIN post_tags AS pt ON pt.tag_id = t.tag_id
GROUP BY t.name
ORDER BY t.name;

DROP SCHEMA test_330 CASCADE;
