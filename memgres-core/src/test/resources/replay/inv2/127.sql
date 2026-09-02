-- source: investigation-2026-08.md
-- finding: 127
-- title: The parser's identifier acceptance is inconsistent: the AS-alias and CTE-name paths take any token at all, while the bare-label path rejects every clause keywor
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_k (a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_k VALUES (1),(2);
-- begin-expected
-- columns: natural:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT a natural FROM zz_k ORDER BY 1;
-- begin-expected
-- columns: s:int4
-- row: 3
-- rowcount: 1
-- end-expected
WITH "A" AS (SELECT 1 AS c), "a" AS (SELECT 2 AS c) SELECT (SELECT c FROM "A") + (SELECT c FROM "a") AS s;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_k" already exists
-- end-expected-error
CREATE TABLE zz_k (a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_k VALUES (1),(2);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "join"
-- end-expected-error
SELECT count(*) FROM zz_k AS join;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "select"
-- end-expected-error
WITH select AS (SELECT 1 AS n) SELECT n FROM select;
