-- source: investigation.md
-- finding: 38
-- title: Scalar subquery arity
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT (SELECT 1, 2 WHERE false);
-- PG: 42601 subquery must return only one column | mg: NULL
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ARRAY(SELECT 1, 2);
-- PG: 42601 | mg: {1}  (second column dropped)
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT ((1, 2) = (SELECT 1, 2))::text;
-- PG: true  | mg: 42601 subquery must return only one column;
