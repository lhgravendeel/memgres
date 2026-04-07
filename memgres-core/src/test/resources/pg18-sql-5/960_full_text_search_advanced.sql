DROP SCHEMA IF EXISTS test_960 CASCADE;
CREATE SCHEMA test_960;
SET search_path TO test_960;

CREATE TABLE docs (
    doc_id integer PRIMARY KEY,
    title text NOT NULL,
    body text NOT NULL
);

INSERT INTO docs VALUES
(1, 'PostgreSQL guide', 'PostgreSQL full text search is powerful and flexible.'),
(2, 'Search tips', 'Phrase search and ranking can improve result quality.'),
(3, 'Cooking notes', 'Bread and soup recipes.');

-- begin-expected
-- columns: doc_id
-- row: 1
-- row: 2
-- end-expected
SELECT doc_id
FROM docs
WHERE to_tsvector('english', title || ' ' || body) @@ plainto_tsquery('english', 'search')
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id
-- row: 2
-- end-expected
SELECT doc_id
FROM docs
WHERE to_tsvector('english', title || ' ' || body) @@ phraseto_tsquery('english', 'phrase search')
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id
-- row: 1
-- row: 2
-- end-expected
SELECT doc_id
FROM docs
WHERE to_tsvector('english', title || ' ' || body) @@ websearch_to_tsquery('english', 'search OR ranking')
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id,rank
-- row: 2|0.0759909
-- row: 1|0.0607927
-- end-expected
SELECT doc_id,
       ts_rank(
         to_tsvector('english', title || ' ' || body),
         plainto_tsquery('english', 'search')
       ) AS rank
FROM docs
WHERE to_tsvector('english', title || ' ' || body) @@ plainto_tsquery('english', 'search')
ORDER BY rank DESC, doc_id;

-- begin-expected
-- columns: headline
-- row: PostgreSQL full text <b>search</b> is powerful and flexible.
-- end-expected
-- Note: ts_headline may include leading words before match within the fragment
SELECT ts_headline(
         'english',
         body,
         plainto_tsquery('english', 'search'),
         'StartSel=<b>, StopSel=</b>'
       ) AS headline
FROM docs
WHERE doc_id = 1;

