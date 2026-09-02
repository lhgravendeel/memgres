-- source: investigation-2026-08.md
-- finding: 121
-- title: A quantified comparison evaluates the left operand's NULL and returns NULL before the right-hand set is inspected, and the row-comparison path inside IN is two-
-- begin-expected
-- columns: text:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT (NULL::int IN (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT (NULL::int NOT IN (SELECT 1 WHERE false))::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT (NULL::int = ALL ('{}'::int[]))::text;
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (VALUES (1),(2),(NULL)) v(x) WHERE x NOT IN (SELECT 1 WHERE false);
-- begin-expected
-- columns: text:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT ((NULL::int, 1) IN (SELECT 1, 2))::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT ((NULL::int, 1) NOT IN (SELECT 1, 2))::text;
-- begin-expected
-- columns: text:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT ((NULL::int, 1) IN ((1,1)))::text;
-- begin-expected
-- columns: text:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT ((NULL::int, 1) IN ((1,2),(2,1)))::text;
-- begin-expected
-- columns: text:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT ((NULL::int, 1) = ANY (SELECT 1, 1))::text;
