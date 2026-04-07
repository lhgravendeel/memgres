DROP SCHEMA IF EXISTS test_990 CASCADE;
CREATE SCHEMA test_990;
SET search_path TO test_990;

CREATE TABLE posts (
    post_id integer PRIMARY KEY,
    tenant_id integer NOT NULL,
    category text NOT NULL,
    deleted_at timestamp,
    title text NOT NULL,
    body text NOT NULL,
    created_at timestamp NOT NULL
);

INSERT INTO posts VALUES
(1, 10, 'db',   NULL, 'PostgreSQL search', 'Full text search tutorial', '2024-01-01 10:00'),
(2, 10, 'db',   NULL, 'Ranking results',   'Search ranking tips',       '2024-01-02 10:00'),
(3, 10, 'misc', NULL, 'Other topic',       'Nothing about search',      '2024-01-03 10:00'),
(4, 20, 'db',   NULL, 'Tenant other',      'Search for another tenant', '2024-01-04 10:00'),
(5, 10, 'db',   '2024-01-05 10:00', 'Deleted search', 'Search hidden post', '2024-01-05 09:00');

-- begin-expected
-- columns: post_id
-- row: 2
-- end-expected
SELECT post_id
FROM posts
WHERE tenant_id = 10
  AND category = 'db'
  AND deleted_at IS NULL
  AND to_tsvector('english', title || ' ' || body) @@ plainto_tsquery('english', 'search ranking')
ORDER BY ts_rank(to_tsvector('english', title || ' ' || body), plainto_tsquery('english', 'search ranking')) DESC,
         created_at DESC,
         post_id DESC;

-- begin-expected
-- columns: post_id
-- row: 1
-- end-expected
SELECT post_id
FROM posts
WHERE tenant_id = 10
  AND deleted_at IS NULL
  AND to_tsvector('english', title || ' ' || body) @@ plainto_tsquery('english', 'search')
ORDER BY created_at, post_id
LIMIT 1 OFFSET 0;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT COUNT(*) AS cnt
FROM posts
WHERE tenant_id = 10
  AND deleted_at IS NULL
  AND to_tsvector('english', title || ' ' || body) @@ plainto_tsquery('english', 'search');

