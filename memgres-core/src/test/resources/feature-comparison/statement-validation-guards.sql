-- Guards PostgreSQL applies before a statement runs: MATCH FULL foreign keys, read-only
-- transactions, WITH TIES without an ordering, and populating an untyped record.

DROP TABLE IF EXISTS svg_f CASCADE;
DROP TABLE IF EXISTS svg_s CASCADE;
DROP TABLE IF EXISTS svg_p CASCADE;
CREATE TABLE svg_p (a int, b int, PRIMARY KEY (a,b));
CREATE TABLE svg_f (x int, y int, FOREIGN KEY (x,y) REFERENCES svg_p(a,b) MATCH FULL);
CREATE TABLE svg_s (x int, y int, FOREIGN KEY (x,y) REFERENCES svg_p(a,b));
INSERT INTO svg_p VALUES (1,1);

-- MATCH FULL: half a key identifies no row.
-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO svg_f VALUES (1,NULL);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO svg_f VALUES (NULL,1);

-- All-NULL is still "references nothing".
INSERT INTO svg_f VALUES (NULL,NULL);

-- A key that exists is still fine.
INSERT INTO svg_f VALUES (1,1);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO svg_f VALUES (9,9);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM svg_f;

-- MATCH SIMPLE, the default, excuses the key on any NULL.
INSERT INTO svg_s VALUES (1,NULL);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM svg_s;

-- WITH TIES needs an ORDER BY to know what ties.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH TIES cannot be specified without ORDER BY
-- end-expected-error
SELECT 1 AS a FETCH FIRST 1 ROWS WITH TIES;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT a FROM svg_p ORDER BY a FETCH FIRST 1 ROWS WITH TIES;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1 AS a FETCH FIRST 1 ROWS ONLY;

-- A bare record names no columns, so there is no shape to populate.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: could not determine row type
-- end-expected-error
SELECT jsonb_populate_record(NULL::record, '{}');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: could not determine row type
-- end-expected-error
SELECT json_populate_record(NULL::record, '{}');

-- A read-only transaction refuses anything that would change the database. An error aborts the
-- transaction, so each rejection gets a transaction of its own.
DROP TABLE IF EXISTS svg_ro CASCADE;
CREATE TABLE svg_ro (i int);
INSERT INTO svg_ro VALUES (1);

BEGIN TRANSACTION READ ONLY;
-- begin-expected-error
-- sqlstate: 25006
-- message-like: read-only transaction
-- end-expected-error
CREATE TABLE svg_ro2 (i int);
ROLLBACK;

BEGIN TRANSACTION READ ONLY;
-- begin-expected-error
-- sqlstate: 25006
-- message-like: read-only transaction
-- end-expected-error
INSERT INTO svg_ro VALUES (7);
ROLLBACK;

BEGIN TRANSACTION READ ONLY;
-- begin-expected-error
-- sqlstate: 25006
-- message-like: read-only transaction
-- end-expected-error
TRUNCATE svg_ro;
ROLLBACK;

BEGIN TRANSACTION READ ONLY;
-- begin-expected-error
-- sqlstate: 25006
-- message-like: read-only transaction
-- end-expected-error
ALTER TABLE svg_ro ADD COLUMN j int;
ROLLBACK;

BEGIN TRANSACTION READ ONLY;
-- begin-expected-error
-- sqlstate: 25006
-- message-like: read-only transaction
-- end-expected-error
DROP TABLE svg_ro;
ROLLBACK;

-- Reading in a read-only transaction is the point of one, and nothing above changed anything.
BEGIN TRANSACTION READ ONLY;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM svg_ro;
ROLLBACK;

DROP TABLE svg_ro;
DROP TABLE svg_f;
DROP TABLE svg_s;
DROP TABLE svg_p;
