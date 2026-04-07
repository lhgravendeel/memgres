-- ============================================================
-- 02: Syntax Errors and Malformed SQL
-- ============================================================

-- Setup
CREATE TABLE syn_test (id serial PRIMARY KEY, name text, val int, active boolean DEFAULT true);
INSERT INTO syn_test (name, val) VALUES ('alpha', 10), ('beta', 20), ('gamma', 30);

-- === Valid baseline queries (should all succeed) ===
SELECT 1;
SELECT 1 AS one;
SELECT 1 + 2;
SELECT 'hello' || ' ' || 'world';
SELECT * FROM syn_test;
SELECT * FROM syn_test WHERE val > 15;
SELECT name FROM syn_test ORDER BY val;
SELECT name FROM syn_test ORDER BY val DESC NULLS LAST;

-- === Misspelled keywords ===
SLECT * FROM syn_test;
SELEC * FROM syn_test;
SELECT * FORM syn_test;
SELECT * FROM syn_test WHER val > 1;
SELECT * FROM syn_test WEHRE val > 1;
CREAT TABLE bad1 (id int);
CRATE TABLE bad2 (id int);
INSRT INTO syn_test (name) VALUES ('x');
INSER INTO syn_test (name) VALUES ('x');
UPDAT syn_test SET val = 1;
DELET FROM syn_test;
DELTE FROM syn_test;

-- === Missing required clauses ===
SELECT * FROM;
SELECT * FROM syn_test WHERE;
SELECT * FROM syn_test ORDER BY;
SELECT * FROM syn_test GROUP BY;
SELECT * FROM syn_test HAVING;
SELECT FROM syn_test WHERE val > 1;
INSERT INTO;
INSERT INTO syn_test VALUES;
INSERT INTO syn_test (name) VALUES;
UPDATE SET val = 1;
UPDATE syn_test SET;
DELETE FROM;
CREATE TABLE;
CREATE TABLE t1;
DROP TABLE;
ALTER TABLE;

-- === Duplicate clauses ===
SELECT * FROM syn_test WHERE val > 1 WHERE val < 100;
SELECT * FROM syn_test ORDER BY val ORDER BY name;
SELECT * FROM syn_test LIMIT 1 LIMIT 2;

-- === Unmatched parentheses ===
SELECT * FROM syn_test WHERE (val > 1;
SELECT * FROM syn_test WHERE val > 1);
INSERT INTO syn_test (name, val VALUES ('x', 1);
INSERT INTO syn_test (name, val) VALUES ('x', 1;
SELECT (1 + 2;
SELECT 1 + 2);
CREATE TABLE bad_parens (id int, name text;

-- === Incomplete expressions ===
SELECT 1 +;
SELECT 1 *;
SELECT * FROM syn_test WHERE val =;
SELECT * FROM syn_test WHERE val = AND name = 'alpha';
SELECT * FROM syn_test WHERE AND val > 1;
SELECT * FROM syn_test WHERE val > 1 AND;
SELECT * FROM syn_test WHERE val BETWEEN;
SELECT * FROM syn_test WHERE val BETWEEN 1 AND;
SELECT * FROM syn_test WHERE val IN;
SELECT * FROM syn_test WHERE val IN ();
SELECT * FROM syn_test WHERE name LIKE;

-- === Invalid identifiers ===
SELECT 123abc FROM syn_test;
CREATE TABLE 123table (id int);

-- === Trailing garbage ===
SELECT 1 BANANA;
SELECT * FROM syn_test POTATO;
INSERT INTO syn_test (name) VALUES ('x') GARBAGE;

-- === Empty/whitespace ===
;

-- === Multiple semicolons ===
SELECT 1;;
SELECT 1;;;

-- === Comments only ===
-- this is just a comment;
/* this is a block comment */;

-- === Valid but unusual ===
SELECT /* inline comment */ 1;
SELECT 1 -- trailing comment
;
SELECT 1 AS "column with spaces";
SELECT 1 AS "123numeric_start";
SELECT 1 AS "";

-- === Operator errors ===
SELECT 1 === 1;
SELECT 1 <> <> 1;
SELECT 1 !! 2;
SELECT * FROM syn_test WHERE val >> 1;

-- Cleanup
DROP TABLE syn_test;
