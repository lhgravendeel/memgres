CREATE TABLE docs (id int, payload jsonb, tags text[], doc tsvector);
CREATE INDEX idx_json ON docs USING gin(payload);
CREATE INDEX idx_tags ON docs USING gin(tags);
CREATE INDEX idx_doc ON docs USING gin(doc);
INSERT INTO docs VALUES (1,'{"a":1}',ARRAY['x','y'],to_tsvector('hello world')),(2,'{"b":2}',ARRAY['y'],to_tsvector('world test'));
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM docs WHERE payload @> '{"a":1}';
