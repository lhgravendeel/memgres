-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Unrelated singletons
-- begin-expected
-- columns: set_config:text
-- row: 17MB
-- rowcount: 1
-- end-expected
UPDATE pg_settings SET setting = '17MB' WHERE name = 'work_mem';
-- begin-expected
-- columns: work_mem:text
-- row: 17MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
