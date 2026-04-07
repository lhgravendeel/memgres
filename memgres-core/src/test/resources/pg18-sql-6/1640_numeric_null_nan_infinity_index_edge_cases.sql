CREATE TABLE num_test (val numeric, fval double precision);
CREATE UNIQUE INDEX idx_num_unique ON num_test(val);
INSERT INTO num_test VALUES (2.00, 1.0);
-- expect-error
INSERT INTO num_test VALUES (2.0, 2.0);
INSERT INTO num_test VALUES (NULL, 'NaN');
INSERT INTO num_test VALUES (NULL, 'Infinity');
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT COUNT(*) FROM num_test;
