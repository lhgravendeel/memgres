DROP SCHEMA IF EXISTS test_120 CASCADE;
CREATE SCHEMA test_120;
SET search_path TO test_120;

CREATE TABLE docs (
    doc_id integer PRIMARY KEY,
    payload jsonb NOT NULL
);

INSERT INTO docs VALUES
    (1, '{"name":"alpha","count":2,"tags":["x","y"],"meta":{"active":true,"score":9}}'),
    (2, '{"name":"beta","count":5,"tags":["z"],"meta":{"active":false,"score":3}}');

-- begin-expected
-- columns: doc_id|name_txt|meta_score|active_txt
-- row: 1|alpha|9|true
-- row: 2|beta|3|false
-- end-expected
SELECT
    doc_id,
    payload->>'name' AS name_txt,
    payload#>>'{meta,score}' AS meta_score,
    payload#>>'{meta,active}' AS active_txt
FROM docs
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id|has_name|contains_meta
-- row: 1|true|true
-- row: 2|true|true
-- end-expected
SELECT
    doc_id,
    payload ? 'name' AS has_name,
    payload @> '{"meta":{}}'::jsonb AS contains_meta
FROM docs
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id|tag
-- row: 1|x
-- row: 1|y
-- row: 2|z
-- end-expected
SELECT d.doc_id, e.value AS tag
FROM docs d
CROSS JOIN LATERAL jsonb_array_elements_text(d.payload->'tags') AS e(value)
ORDER BY d.doc_id, e.value;

UPDATE docs
SET payload = jsonb_set(payload, '{meta,score}', to_jsonb(10), false)
WHERE doc_id = 1;

-- begin-expected
-- columns: merged
-- row: {"n": 1, "name": "gamma", "extra": "ok"}
-- end-expected
SELECT jsonb_build_object('name', 'gamma', 'n', 1) || '{"extra":"ok"}'::jsonb AS merged;

-- begin-expected
-- columns: doc_id|score
-- row: 1|10
-- row: 2|3
-- end-expected
SELECT doc_id, (payload#>>'{meta,score}')::integer AS score
FROM docs
ORDER BY doc_id;

DROP SCHEMA test_120 CASCADE;
