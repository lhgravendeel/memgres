DROP SCHEMA IF EXISTS test_1000 CASCADE;
CREATE SCHEMA test_1000;
SET search_path TO test_1000;

CREATE TABLE items (
    item_id integer PRIMARY KEY,
    item_name text NOT NULL
);

INSERT INTO items VALUES
(1, 'alpha'),
(2, 'beta'),
(3, 'gamma'),
(4, 'delta');

-- begin-expected
-- columns: item_id,item_name
-- row: 1|alpha
-- row: 3|gamma
-- end-expected
SELECT item_id, item_name
FROM items
WHERE item_id = ANY(ARRAY[1,3])
ORDER BY item_id;

-- begin-expected
-- columns: item_id,item_name
-- end-expected
SELECT item_id, item_name
FROM items
WHERE item_id = ANY(ARRAY[]::integer[])
ORDER BY item_id;

-- begin-expected
-- columns: ord,item_id,item_name
-- row: 1|3|gamma
-- row: 2|1|alpha
-- row: 3|4|delta
-- end-expected
SELECT u.ord, i.item_id, i.item_name
FROM unnest(ARRAY[3,1,4]) WITH ORDINALITY AS u(item_id, ord)
JOIN items i USING (item_id)
ORDER BY u.ord;

-- begin-expected
-- columns: item_id,item_name
-- row: 2|beta
-- row: 4|delta
-- end-expected
SELECT i.item_id, i.item_name
FROM items i
JOIN (VALUES (2), (4)) AS v(item_id)
  ON i.item_id = v.item_id
ORDER BY i.item_id;

-- begin-expected
-- columns: pair_label
-- row: 1-alpha
-- row: 2-beta
-- end-expected
SELECT format('%s-%s', p.item_id, i.item_name) AS pair_label
FROM (VALUES (1), (2)) AS p(item_id)
JOIN items i USING (item_id)
ORDER BY p.item_id;

