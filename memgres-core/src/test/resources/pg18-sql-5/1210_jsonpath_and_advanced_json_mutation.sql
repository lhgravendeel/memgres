DROP SCHEMA IF EXISTS test_1210 CASCADE;
CREATE SCHEMA test_1210;
SET search_path TO test_1210;

CREATE TABLE docs (
    doc_id integer PRIMARY KEY,
    payload jsonb NOT NULL
);

INSERT INTO docs VALUES
(1, '{"name":"alpha","items":[{"qty":2},{"qty":5}],"active":true,"meta":{"owner":"u1"}}'),
(2, '{"name":"beta","items":[{"qty":1}],"active":false,"meta":{"owner":"u2"}}'),
(3, '{"name":"gamma","items":[],"meta":{"owner":null}}');

-- begin-expected
-- columns: doc_id
-- row: 1
-- end-expected
SELECT doc_id
FROM docs
WHERE payload @? '$.items[*] ? (@.qty > 3)'
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id,matches
-- row: 1|t
-- row: 2|f
-- row: 3|
-- end-expected
SELECT doc_id,
       payload @@ '$.active == true' AS matches
FROM docs
ORDER BY doc_id;

-- begin-expected
-- columns: qty_text
-- row: 2
-- row: 5
-- end-expected
SELECT jsonb_path_query(payload, '$.items[*].qty')::text AS qty_text
FROM docs
WHERE doc_id = 1
ORDER BY qty_text;

UPDATE docs
SET payload = jsonb_set(payload, '{meta,owner}', '"u9"'::jsonb, true)
WHERE doc_id = 3;

-- begin-expected
-- columns: owner
-- row: u9
-- end-expected
SELECT payload #>> '{meta,owner}' AS owner
FROM docs
WHERE doc_id = 3;

UPDATE docs
SET payload = jsonb_insert(payload, '{items,0}', '{"qty":9}'::jsonb, true)
WHERE doc_id = 2;

-- begin-expected
-- columns: first_qty
-- row: 1
-- end-expected
SELECT payload #>> '{items,0,qty}' AS first_qty
FROM docs
WHERE doc_id = 2;

UPDATE docs
SET payload = payload - 'active'
WHERE doc_id = 1;

-- begin-expected
-- columns: has_active_key
-- row: f
-- end-expected
SELECT payload ? 'active' AS has_active_key
FROM docs
WHERE doc_id = 1;

