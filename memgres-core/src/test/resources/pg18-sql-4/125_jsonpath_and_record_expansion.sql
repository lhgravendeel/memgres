DROP SCHEMA IF EXISTS test_125 CASCADE;
CREATE SCHEMA test_125;
SET search_path TO test_125;

CREATE TABLE docs (
    doc_id integer PRIMARY KEY,
    payload jsonb NOT NULL
);

INSERT INTO docs(doc_id, payload) VALUES
    (1, '{"user":{"name":"anna","active":true},"items":[{"sku":"A1","qty":2},{"sku":"B1","qty":1}],"meta":{"priority":5},"misc":null}'),
    (2, '{"user":{"name":"bob","active":false},"items":[{"sku":"C1","qty":4}],"meta":{"priority":2}}'),
    (3, '{"user":{"name":"cara","active":true},"items":[],"meta":{"priority":7},"misc":null}');

-- begin-expected
-- columns: doc_id|has_priority_ge_5
-- row: 1|t
-- row: 2|f
-- row: 3|t
-- end-expected
SELECT doc_id,
       payload @@ '$.meta.priority >= 5' AS has_priority_ge_5
FROM docs
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id|matched_sku
-- row: 1|A1
-- row: 2|C1
-- end-expected
SELECT d.doc_id, q.matched_sku
FROM docs AS d
CROSS JOIN LATERAL jsonb_path_query(d.payload, '$.items[*] ? (@.qty >= 2).sku') AS q(matched_sku)
ORDER BY d.doc_id, q.matched_sku::text;

-- begin-expected
-- columns: doc_id|sku|qty
-- row: 1|A1|2
-- row: 1|B1|1
-- row: 2|C1|4
-- end-expected
SELECT d.doc_id, r.sku, r.qty
FROM docs AS d
CROSS JOIN LATERAL jsonb_to_recordset(d.payload->'items') AS r(sku text, qty integer)
ORDER BY d.doc_id, r.sku;

-- begin-expected
-- columns: doc_id|user_name|active|priority
-- row: 1|anna|t|5
-- row: 2|bob|f|2
-- row: 3|cara|t|7
-- end-expected
SELECT d.doc_id, r.user_name, r.active, r.priority
FROM docs AS d
CROSS JOIN LATERAL jsonb_to_record(
    jsonb_build_object(
        'user_name', d.payload #>> '{user,name}',
        'active', (d.payload #>> '{user,active}')::boolean,
        'priority', (d.payload #>> '{meta,priority}')::integer
    )
) AS r(user_name text, active boolean, priority integer)
ORDER BY d.doc_id;

-- begin-expected
-- columns: doc_id|cleaned_payload
-- row: 1|{"meta": {"priority": 5}, "user": {"name": "anna", "active": true}, "items": [{"qty": 2, "sku": "A1"}, {"qty": 1, "sku": "B1"}]}
-- row: 2|{"meta": {"priority": 2}, "user": {"name": "bob", "active": false}, "items": [{"qty": 4, "sku": "C1"}]}
-- row: 3|{"meta": {"priority": 7}, "user": {"name": "cara", "active": true}, "items": []}
-- end-expected
SELECT doc_id, jsonb_strip_nulls(payload) AS cleaned_payload
FROM docs
ORDER BY doc_id;

-- begin-expected
-- columns: doc_id|augmented
-- row: 1|{"meta": {"priority": 6}, "misc": null, "user": {"name": "anna", "active": true}, "items": [{"qty": 2, "sku": "A1"}, {"qty": 1, "sku": "B1"}], "status": "seen"}
-- row: 2|{"meta": {"priority": 3}, "user": {"name": "bob", "active": false}, "items": [{"qty": 4, "sku": "C1"}], "status": "seen"}
-- row: 3|{"meta": {"priority": 8}, "misc": null, "user": {"name": "cara", "active": true}, "items": [], "status": "seen"}
-- end-expected
SELECT doc_id,
       jsonb_set(payload || '{"status":"seen"}'::jsonb,
                 '{meta,priority}',
                 to_jsonb(((payload #>> '{meta,priority}')::integer + 1))) AS augmented
FROM docs
ORDER BY doc_id;

DROP SCHEMA test_125 CASCADE;
