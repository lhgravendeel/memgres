DROP SCHEMA IF EXISTS test_550 CASCADE;
CREATE SCHEMA test_550;
SET search_path TO test_550;

CREATE TABLE articles (
    article_id integer PRIMARY KEY,
    title text NOT NULL,
    status text NOT NULL,
    pinned boolean NOT NULL,
    score integer NOT NULL,
    created_at timestamp NOT NULL,
    updated_at timestamp,
    slug text NOT NULL
);

INSERT INTO articles VALUES
    (1, 'alpha docs',   'published', false, 10, TIMESTAMP '2024-01-01 09:00:00', TIMESTAMP '2024-01-05 09:00:00', 'alpha-docs'),
    (2, 'beta docs',    'draft',     false,  4, TIMESTAMP '2024-01-02 09:00:00', NULL,                              'beta-docs'),
    (3, 'gamma guide',  'published', true,   8, TIMESTAMP '2024-01-03 09:00:00', TIMESTAMP '2024-01-03 12:00:00', 'gamma-guide'),
    (4, 'alpha guide',  'published', false,  7, TIMESTAMP '2024-01-04 09:00:00', TIMESTAMP '2024-01-07 09:00:00', 'alpha-guide'),
    (5, 'zeta alpha',   'archived',  false,  6, TIMESTAMP '2024-01-05 09:00:00', TIMESTAMP '2024-01-06 09:00:00', 'zeta-alpha');

-- begin-expected
-- columns: article_id|title
-- row: 3|gamma guide
-- row: 4|alpha guide
-- row: 5|zeta alpha
-- row: 1|alpha docs
-- row: 2|beta docs
-- end-expected
SELECT article_id, title
FROM articles
ORDER BY pinned DESC, COALESCE(updated_at, created_at) DESC, article_id DESC;

-- begin-expected
-- columns: article_id|title|sort_mode
-- row: 4|alpha guide|newest
-- row: 3|gamma guide|newest
-- row: 1|alpha docs|newest
-- end-expected
SELECT article_id, title, 'newest' AS sort_mode
FROM articles
WHERE status = 'published'
ORDER BY created_at DESC, article_id DESC
LIMIT 3;

-- begin-expected
-- columns: article_id|title|rank_bucket
-- row: 1|alpha docs|exact_prefix
-- row: 4|alpha guide|exact_prefix
-- row: 5|zeta alpha|substring
-- end-expected
SELECT article_id,
       title,
       CASE
           WHEN slug LIKE 'alpha%' THEN 'exact_prefix'
           WHEN lower(title) LIKE '%alpha%' THEN 'substring'
           ELSE 'other'
       END AS rank_bucket
FROM articles
WHERE slug LIKE 'alpha%'
   OR lower(title) LIKE '%alpha%'
ORDER BY CASE
             WHEN slug LIKE 'alpha%' THEN 1
             WHEN lower(title) LIKE '%alpha%' THEN 2
             ELSE 3
         END,
         score DESC,
         article_id;

-- begin-expected
-- columns: article_id|title|requested_position
-- row: 4|alpha guide|1
-- row: 1|alpha docs|2
-- row: 3|gamma guide|3
-- end-expected
WITH requested(article_id, ord) AS (
    VALUES (4, 1), (1, 2), (3, 3)
)
SELECT a.article_id, a.title, r.ord AS requested_position
FROM requested r
JOIN articles a ON a.article_id = r.article_id
ORDER BY r.ord;

DROP SCHEMA test_550 CASCADE;
