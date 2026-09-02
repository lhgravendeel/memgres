-- source: investigation-2026-08.md
-- finding: 40
-- title: The JSON text validator's recursion guard is an order of magnitude tighter than PostgreSQL's stack-byte-based limit.
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
