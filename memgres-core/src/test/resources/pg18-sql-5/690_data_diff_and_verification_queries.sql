DROP SCHEMA IF EXISTS test_690 CASCADE;
CREATE SCHEMA test_690;
SET search_path TO test_690;

CREATE TABLE source_data (
    id integer PRIMARY KEY,
    value text NOT NULL
);

CREATE TABLE target_data (
    id integer PRIMARY KEY,
    value text NOT NULL
);

INSERT INTO source_data(id, value) VALUES
(1, 'a'),
(2, 'b'),
(3, 'c');

INSERT INTO target_data(id, value) VALUES
(1, 'a'),
(2, 'x'),
(4, 'd');

-- begin-expected
-- columns: id,value
-- row: 2|b
-- row: 3|c
-- end-expected
SELECT id, value
FROM source_data
EXCEPT
SELECT id, value
FROM target_data
ORDER BY id;

-- begin-expected
-- columns: id,value
-- row: 2|x
-- row: 4|d
-- end-expected
SELECT id, value
FROM target_data
EXCEPT
SELECT id, value
FROM source_data
ORDER BY id;

-- begin-expected
-- columns: id,source_value,target_value
-- row: 2|b|x
-- end-expected
SELECT s.id, s.value AS source_value, t.value AS target_value
FROM source_data s
JOIN target_data t USING (id)
WHERE s.value <> t.value
ORDER BY s.id;

-- begin-expected
-- columns: source_count,target_count
-- row: 3|3
-- end-expected
SELECT
  (SELECT COUNT(*) FROM source_data) AS source_count,
  (SELECT COUNT(*) FROM target_data) AS target_count;

