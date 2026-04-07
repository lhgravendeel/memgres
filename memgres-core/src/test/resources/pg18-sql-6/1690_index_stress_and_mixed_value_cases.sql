CREATE TABLE mix_idx (a int, b text, c numeric);
CREATE INDEX idx_mix ON mix_idx(a,b,c);
INSERT INTO mix_idx VALUES (1,'x',1.0),(1,'x',1.00),(2,NULL,NULL);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT COUNT(*) FROM mix_idx;
