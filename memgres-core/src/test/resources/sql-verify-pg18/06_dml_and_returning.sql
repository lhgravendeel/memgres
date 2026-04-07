\echo '=== 06_dml_and_returning.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

CREATE TABLE dml_t(
    id int PRIMARY KEY,
    x int,
    y text DEFAULT 'd',
    z timestamp DEFAULT current_timestamp
);
INSERT INTO dml_t(id, x, y) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');
SELECT * FROM dml_t ORDER BY id;

INSERT INTO dml_t(id, x) VALUES (4, 40) RETURNING id, x, y, pg_typeof(y);
INSERT INTO dml_t AS t(id, x, y)
VALUES (5, 50, 'e')
RETURNING t.id, t.y;

UPDATE dml_t SET x = x + 1 WHERE id IN (1,2) RETURNING id, x ORDER BY id;
UPDATE dml_t SET y = upper(y) WHERE id = 3 RETURNING id, y;
UPDATE dml_t AS t SET x = s.new_x
FROM (VALUES (1, 100), (2, 200)) AS s(id, new_x)
WHERE t.id = s.id
RETURNING t.id, t.x ORDER BY t.id;

DELETE FROM dml_t WHERE id = 5 RETURNING id, y;
DELETE FROM dml_t WHERE id = 999 RETURNING id;

MERGE INTO dml_t AS t
USING (VALUES (1, 111), (6, 60)) AS s(id, x)
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET x = s.x
WHEN NOT MATCHED THEN INSERT (id, x, y) VALUES (s.id, s.x, 'merged');

SELECT * FROM dml_t ORDER BY id;

-- DML errors
INSERT INTO dml_t VALUES (1, 9, 'dup');
INSERT INTO dml_t(id) VALUES (NULL);
INSERT INTO dml_t(no_such_col) VALUES (1);
INSERT INTO dml_t(id, x, y) VALUES (7, 'x', 'bad-type');
INSERT INTO dml_t(id, x, y) VALUES (8, 80);
INSERT INTO dml_t SELECT 9;
UPDATE dml_t SET no_such_col = 1 WHERE id = 1;
UPDATE dml_t SET x = 'bad' WHERE id = 1;
UPDATE dml_t SET x = (SELECT x FROM dml_t) WHERE id = 1;
UPDATE dml_t SET x = x + 1 FROM dml_t t2 WHERE dml_t.id = t2.id;
DELETE FROM dml_t WHERE no_such_col = 1;
DELETE FROM no_such_table;
MERGE INTO dml_t USING dml_t ON (dml_t.id = dml_t.id) WHEN MATCHED THEN UPDATE SET x = 0;
MERGE INTO dml_t AS t USING (VALUES (1)) AS s(id) ON t.id = s.id WHEN MATCHED THEN UPDATE SET no_such_col = 1;

DROP SCHEMA compat CASCADE;
