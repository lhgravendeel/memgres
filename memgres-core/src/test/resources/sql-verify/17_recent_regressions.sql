-- SQL verification for recent fix regressions (H38, H39, M1, M24, M28, L1)

-- H38: jsonb column || jsonb column
CREATE TABLE v17_j (a jsonb, b jsonb);
INSERT INTO v17_j VALUES ('1', '2');
INSERT INTO v17_j VALUES ('[1]', '"x"');
SELECT a || b FROM v17_j ORDER BY a::text;
-- expected: [1, 2] and [1, "x"]
DROP TABLE v17_j;

-- H39: advisory xact lock released in autocommit
SELECT pg_advisory_xact_lock(77777);
SELECT pg_try_advisory_xact_lock(77777);
-- expected: true (lock was released after first statement)

-- M1: jsonb scalar -> N returns NULL, not char
SELECT '123'::jsonb -> 0;
-- expected: NULL
SELECT '"abc"'::jsonb -> 0;
-- expected: NULL

-- M28: PREPARE rejects DDL
PREPARE v17_p AS CREATE TABLE v17_x (id int); -- expected-error: 42601
-- expected-error means this should fail

-- L1: alias-hiding error text
CREATE TABLE v17_alias (id int);
SELECT v17_alias.id FROM v17_alias AS x; -- expected-error: 42P01
-- error message should say "invalid reference to FROM-clause entry"
DROP TABLE IF EXISTS v17_alias;
