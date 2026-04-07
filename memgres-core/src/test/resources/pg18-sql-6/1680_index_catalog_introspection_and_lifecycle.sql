CREATE TABLE introspect (id int);
CREATE INDEX idx_i ON introspect(id);
-- begin-expected
-- columns: indexname
-- row: idx_i
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename='introspect';
