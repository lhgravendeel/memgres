-- ON CONFLICT ... DO UPDATE with EXCLUDED pseudo-table
-- Tests various expression types with EXCLUDED references

-- setup
CREATE TABLE oc_test (id int PRIMARY KEY, v text, n int);
INSERT INTO oc_test VALUES (1, 'original', 10);

-- stmt 1: simple EXCLUDED.v
INSERT INTO oc_test VALUES (1, 'updated', 10) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;

-- begin-expected
-- columns: v
-- row: updated
-- end-expected
SELECT v FROM oc_test WHERE id = 1;

-- stmt 2: reset + EXCLUDED with function
UPDATE oc_test SET v = 'original' WHERE id = 1;
INSERT INTO oc_test VALUES (1, 'hello', 10) ON CONFLICT (id) DO UPDATE SET v = upper(EXCLUDED.v);

-- begin-expected
-- columns: v
-- row: HELLO
-- end-expected
SELECT v FROM oc_test WHERE id = 1;

-- stmt 3: reset + EXCLUDED with cast
UPDATE oc_test SET v = 'original', n = 10 WHERE id = 1;
INSERT INTO oc_test VALUES (1, 'x', 99) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n::int;

-- begin-expected
-- columns: n
-- row: 99
-- end-expected
SELECT n FROM oc_test WHERE id = 1;

-- stmt 4: reset + EXCLUDED with coalesce
UPDATE oc_test SET v = 'fallback', n = 10 WHERE id = 1;
INSERT INTO oc_test (id, n) VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = COALESCE(EXCLUDED.v, oc_test.v);

-- begin-expected
-- columns: v
-- row: fallback
-- end-expected
SELECT v FROM oc_test WHERE id = 1;

-- stmt 5: reset + arithmetic with EXCLUDED and table ref
UPDATE oc_test SET n = 10 WHERE id = 1;
INSERT INTO oc_test VALUES (1, 'x', 5) ON CONFLICT (id) DO UPDATE SET n = oc_test.n + EXCLUDED.n;

-- begin-expected
-- columns: n
-- row: 15
-- end-expected
SELECT n FROM oc_test WHERE id = 1;

-- stmt 6: reset + CASE with EXCLUDED
UPDATE oc_test SET n = 10 WHERE id = 1;
INSERT INTO oc_test VALUES (1, 'x', 5) ON CONFLICT (id) DO UPDATE SET n = CASE WHEN EXCLUDED.n > oc_test.n THEN EXCLUDED.n ELSE oc_test.n END;

-- begin-expected
-- columns: n
-- row: 10
-- end-expected
SELECT n FROM oc_test WHERE id = 1;

-- stmt 7: reset + EXCLUDED in WHERE clause (does not pass)
UPDATE oc_test SET n = 10 WHERE id = 1;
INSERT INTO oc_test VALUES (1, 'x', 5) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n > oc_test.n;

-- begin-expected
-- columns: n
-- row: 10
-- end-expected
SELECT n FROM oc_test WHERE id = 1;

-- stmt 8: EXCLUDED in WHERE clause (passes)
INSERT INTO oc_test VALUES (1, 'x', 20) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n > oc_test.n;

-- begin-expected
-- columns: n
-- row: 20
-- end-expected
SELECT n FROM oc_test WHERE id = 1;

-- stmt 9: reset + concat with EXCLUDED
UPDATE oc_test SET v = 'hello' WHERE id = 1;
INSERT INTO oc_test VALUES (1, 'world', 0) ON CONFLICT (id) DO UPDATE SET v = oc_test.v || ' ' || EXCLUDED.v;

-- begin-expected
-- columns: v
-- row: hello world
-- end-expected
SELECT v FROM oc_test WHERE id = 1;

-- stmt 10: reset + nested function with EXCLUDED
UPDATE oc_test SET v = 'old' WHERE id = 1;
INSERT INTO oc_test VALUES (1, '  HELLO  ', 0) ON CONFLICT (id) DO UPDATE SET v = lower(trim(EXCLUDED.v));

-- begin-expected
-- columns: v
-- row: hello
-- end-expected
SELECT v FROM oc_test WHERE id = 1;

-- stmt 11: reset + EXCLUDED with IS NULL in CASE
UPDATE oc_test SET v = 'keep' WHERE id = 1;
INSERT INTO oc_test (id, n) VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET v = CASE WHEN EXCLUDED.v IS NULL THEN 'was_null' ELSE EXCLUDED.v END;

-- begin-expected
-- columns: v
-- row: was_null
-- end-expected
SELECT v FROM oc_test WHERE id = 1;

-- stmt 12: reset + multiple columns with EXCLUDED
UPDATE oc_test SET v = 'old', n = 0 WHERE id = 1;
INSERT INTO oc_test VALUES (1, 'new', 42) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, n = EXCLUDED.n;

-- begin-expected
-- columns: v | n
-- row: new, 42
-- end-expected
SELECT v, n FROM oc_test WHERE id = 1;

-- stmt 13: reset + RETURNING with EXCLUDED
UPDATE oc_test SET v = 'old', n = 0 WHERE id = 1;

-- begin-expected
-- columns: v | n
-- row: ret, 99
-- end-expected
INSERT INTO oc_test VALUES (1, 'ret', 99) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, n = EXCLUDED.n RETURNING v, n;

-- cleanup
DROP TABLE oc_test;
