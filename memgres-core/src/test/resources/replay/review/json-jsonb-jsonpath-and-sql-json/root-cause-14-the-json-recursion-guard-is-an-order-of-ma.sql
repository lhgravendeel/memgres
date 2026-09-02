-- source: review-2026-08.md
-- finding: Root cause 14: the JSON recursion guard is an order of magnitude tighter than PostgreSQL's
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 14: the JSON recursion guard is an order of magnitude tighter than PostgreSQL's
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (repeat('[', 1001) || repeat(']', 1001))::jsonb IS NOT NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (repeat('[', 4000) || repeat(']', 4000))::json IS NOT NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (repeat('{"a":', 1500) || '1' || repeat('}', 1500))::jsonb IS NOT NULL;
