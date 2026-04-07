CREATE TABLE texts (t text);
CREATE INDEX idx_text_pattern ON texts(t text_pattern_ops);
INSERT INTO texts VALUES ('abc'),('abcd'),('xyz');
-- begin-expected
-- columns: t
-- row: abc
-- row: abcd
-- end-expected
SELECT t FROM texts WHERE t LIKE 'abc%';
