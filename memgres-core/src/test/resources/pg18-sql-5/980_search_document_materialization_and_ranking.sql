DROP SCHEMA IF EXISTS test_980 CASCADE;
CREATE SCHEMA test_980;
SET search_path TO test_980;

CREATE TABLE articles (
    article_id integer PRIMARY KEY,
    title text NOT NULL,
    body text NOT NULL,
    tags text NOT NULL,
    search_doc tsvector NOT NULL
);

INSERT INTO articles(article_id, title, body, tags, search_doc)
SELECT article_id, title, body, tags,
       setweight(to_tsvector('english', title), 'A') ||
       setweight(to_tsvector('english', body), 'B') ||
       setweight(to_tsvector('english', tags), 'C')
FROM (VALUES
    (1, 'PostgreSQL search', 'Ranking results matters for users', 'database search'),
    (2, 'Search basics', 'Search ranking ranking', 'tutorial search'),
    (3, 'Gardening', 'Plants and soil care', 'plants home')
) AS v(article_id, title, body, tags);

-- begin-expected
-- columns: article_id
-- row: 1
-- row: 2
-- end-expected
SELECT article_id
FROM articles
WHERE search_doc @@ plainto_tsquery('english', 'search')
ORDER BY article_id;

-- begin-expected
-- columns: article_id,rank
-- row: 2|0.970786
-- row: 1|0.716825
-- end-expected
SELECT article_id,
       ts_rank(search_doc, plainto_tsquery('english', 'search ranking')) AS rank
FROM articles
WHERE search_doc @@ plainto_tsquery('english', 'search ranking')
ORDER BY rank DESC, article_id;

UPDATE articles
SET search_doc =
       setweight(to_tsvector('english', title), 'A') ||
       setweight(to_tsvector('english', body), 'B') ||
       setweight(to_tsvector('english', tags), 'C')
WHERE article_id = 1;

-- begin-expected
-- columns: is_nonempty
-- row: t
-- end-expected
SELECT (search_doc <> ''::tsvector) AS is_nonempty
FROM articles
WHERE article_id = 1;

