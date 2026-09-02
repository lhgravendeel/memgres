-- source: review-2026-08.md
-- finding: Root cause 12: set_config and current_setting stringify their arguments with String.valueOf
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 12: set_config and current_setting stringify their arguments with String.valueOf
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '25MB';
-- begin-expected
-- columns: set_config:text
-- row: 4MB
-- rowcount: 1
-- end-expected
SELECT set_config('work_mem', NULL, false);
-- begin-expected
-- columns: work_mem:text
-- row: 4MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT current_setting(NULL) IS NULL;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: SET requires parameter name
-- end-expected-error
SELECT set_config(NULL, '1MB', false);
