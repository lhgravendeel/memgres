DROP SCHEMA IF EXISTS test_055 CASCADE;
CREATE SCHEMA test_055;
SET search_path TO test_055;

CREATE TABLE people (
    person_id integer PRIMARY KEY,
    grp text NOT NULL,
    name text NOT NULL,
    score integer,
    tags text[] NOT NULL
);

INSERT INTO people(person_id, grp, name, score, tags) VALUES
    (1, 'A', 'anna', 10, ARRAY['red','blue']),
    (2, 'A', 'andy', NULL, ARRAY['green']),
    (3, 'B', 'beth', 20, ARRAY['red']),
    (4, 'B', 'bill', 15, ARRAY['yellow','blue']),
    (5, 'C', 'cara', 15, ARRAY['red','yellow']);

-- begin-expected
-- columns: person_id|name|score
-- row: 3|beth|20
-- row: 4|bill|15
-- row: 1|anna|10
-- end-expected
SELECT DISTINCT ON (score) person_id, name, score
FROM people
WHERE score IS NOT NULL
ORDER BY score DESC, person_id;

-- begin-expected
-- columns: val|label
-- row: 1|one
-- row: 2|two
-- row: 3|three
-- end-expected
SELECT *
FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS v(val, label)
ORDER BY val;

-- begin-expected
-- columns: grp|max_name
-- row: A|red
-- row: B|yellow
-- row: C|yellow
-- end-expected
SELECT p.grp, x.max_name
FROM (SELECT DISTINCT grp FROM people) AS p
CROSS JOIN LATERAL (
    SELECT max(tag) AS max_name
    FROM unnest((SELECT array_agg(t.tag ORDER BY t.tag)
                 FROM people p2
                 CROSS JOIN LATERAL unnest(p2.tags) AS t(tag)
                 WHERE p2.grp = p.grp)) AS u(tag)
) AS x
ORDER BY p.grp;

-- begin-expected
-- columns: person_id|ord|tag
-- row: 1|1|red
-- row: 1|2|blue
-- row: 2|1|green
-- row: 3|1|red
-- row: 4|1|yellow
-- row: 4|2|blue
-- row: 5|1|red
-- row: 5|2|yellow
-- end-expected
SELECT p.person_id, t.ord, t.tag
FROM people AS p
CROSS JOIN LATERAL unnest(p.tags) WITH ORDINALITY AS t(tag, ord)
ORDER BY p.person_id, t.ord;

-- begin-expected
-- columns: person_id|name
-- row: 1|anna
-- row: 3|beth
-- row: 4|bill
-- end-expected
SELECT person_id, name
FROM people
WHERE (grp, score) IN (VALUES ('A', 10), ('B', 20), ('B', 15))
ORDER BY person_id;

-- begin-expected
-- columns: person_id|name|score
-- row: 1|anna|10
-- row: 4|bill|15
-- row: 5|cara|15
-- row: 3|beth|20
-- row: 2|andy|
-- end-expected
SELECT person_id, name, score
FROM people
ORDER BY score NULLS LAST, person_id;

-- begin-expected
-- columns: person_id|name
-- row: 1|anna
-- row: 2|andy
-- end-expected
TABLE (
    SELECT person_id, name
    FROM people
    WHERE grp = 'A'
    ORDER BY person_id
);

DROP SCHEMA test_055 CASCADE;
