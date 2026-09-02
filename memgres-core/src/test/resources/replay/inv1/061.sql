-- source: investigation.md
-- finding: 61
-- title: `ORDER BY` a bare constant
-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS a ORDER BY NULL;
-- PG: 42601 non-integer constant in ORDER BY | mg: accepted;
