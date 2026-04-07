CREATE TABLE t_basic (id int, val int, extra text);
INSERT INTO t_basic VALUES (1,10,'a'),(2,10,'b'),(3,NULL,'c');
CREATE UNIQUE INDEX idx_basic_unique ON t_basic(id);
CREATE INDEX idx_basic_val ON t_basic(val);
CREATE INDEX idx_basic_include ON t_basic(val) INCLUDE (extra);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT COUNT(*) FROM t_basic;
