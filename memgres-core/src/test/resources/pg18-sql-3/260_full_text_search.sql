DROP SCHEMA IF EXISTS test_260 CASCADE;
CREATE SCHEMA test_260;
SET search_path TO test_260;

SET default_text_search_config = 'pg_catalog.english';

CREATE TABLE articles (
    id integer PRIMARY KEY,
    title text,
    body text
);

INSERT INTO articles VALUES
    (1, 'Postgres search', 'PostgreSQL provides powerful full text search'),
    (2, 'Cats and dogs', 'Cats are playful animals'),
    (3, 'Search ranking', 'Text search ranking can be useful');

-- begin-expected
-- columns: vector_text
-- row: 'full':4 'postgresql':1 'power':3 'provid':2 'search':6 'text':5
-- end-expected
SELECT to_tsvector('english', 'PostgreSQL provides powerful full text search')::text AS vector_text;

-- begin-expected
-- columns: id|title
-- row: 1|Postgres search
-- row: 3|Search ranking
-- end-expected
SELECT id, title
FROM articles
WHERE to_tsvector('english', body) @@ plainto_tsquery('english', 'search')
ORDER BY id;

-- begin-expected
-- columns: id|rank_bucket
-- row: 1|high
-- row: 3|high
-- end-expected
SELECT
    id,
    CASE WHEN ts_rank(to_tsvector('english', body), plainto_tsquery('english', 'search')) > 0
         THEN 'high' ELSE 'none' END AS rank_bucket
FROM articles
WHERE to_tsvector('english', body) @@ plainto_tsquery('english', 'search')
ORDER BY id;

DROP SCHEMA test_260 CASCADE;
