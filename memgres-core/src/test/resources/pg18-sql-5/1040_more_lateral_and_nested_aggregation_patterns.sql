DROP SCHEMA IF EXISTS test_1040 CASCADE;
CREATE SCHEMA test_1040;
SET search_path TO test_1040;

CREATE TABLE parents (
    parent_id integer PRIMARY KEY,
    parent_name text NOT NULL
);

CREATE TABLE children (
    child_id integer PRIMARY KEY,
    parent_id integer NOT NULL,
    score integer NOT NULL,
    tags jsonb NOT NULL
);

INSERT INTO parents VALUES
(1, 'p1'),
(2, 'p2');

INSERT INTO children VALUES
(10, 1, 7, '["a","b"]'),
(11, 1, 5, '["b"]'),
(12, 1, 9, '["c"]'),
(20, 2, 4, '["a","d"]'),
(21, 2, 8, '["d"]');

-- begin-expected
-- columns: parent_id,best_child_id,best_score
-- row: 1|12|9
-- row: 2|21|8
-- end-expected
SELECT p.parent_id, x.child_id AS best_child_id, x.score AS best_score
FROM parents p
JOIN LATERAL (
    SELECT child_id, score
    FROM children c
    WHERE c.parent_id = p.parent_id
    ORDER BY score DESC, child_id
    LIMIT 1
) x ON true
ORDER BY p.parent_id;

-- begin-expected
-- columns: parent_id,tag,ord
-- row: 1|a|1
-- row: 1|b|2
-- row: 2|a|1
-- row: 2|d|2
-- end-expected
SELECT p.parent_id, jt.tag, jt.ord
FROM parents p
JOIN LATERAL (
    SELECT tag, ord
    FROM jsonb_array_elements_text((
        SELECT tags
        FROM children c
        WHERE c.parent_id = p.parent_id
        ORDER BY child_id
        LIMIT 1
    )) WITH ORDINALITY AS t(tag, ord)
) jt ON true
ORDER BY p.parent_id, jt.ord;

-- begin-expected
-- columns: parent_id,total_score
-- row: 1|21
-- row: 2|12
-- end-expected
SELECT p.parent_id, agg.total_score
FROM parents p
JOIN LATERAL (
    SELECT SUM(score) AS total_score
    FROM children c
    WHERE c.parent_id = p.parent_id
) agg ON true
ORDER BY p.parent_id;

