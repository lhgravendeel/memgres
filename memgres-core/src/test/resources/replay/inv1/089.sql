-- source: investigation.md
-- finding: 89
-- title: Deep JSON nesting raises `StackOverflowError`
-- begin-expected-error
-- sqlstate: 54001
-- message-like: stack depth limit exceeded
-- end-expected-error
SELECT (repeat('[', 20000) || repeat(']', 20000))::jsonb IS NOT NULL;
--   PG: 54001 stack depth limit exceeded  | mg: XX000 Internal error: StackOverflowError;
