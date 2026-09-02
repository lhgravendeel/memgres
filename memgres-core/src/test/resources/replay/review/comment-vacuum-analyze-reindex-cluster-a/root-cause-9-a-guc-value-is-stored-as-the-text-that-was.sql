-- source: review-2026-08.md
-- finding: Root cause 9: a GUC value is stored as the text that was written, and only enums are canonicalised
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 9: a GUC value is stored as the text that was written, and only enums are canonicalised
-- begin-expected
-- ok: 0
-- end-expected
SET enable_seqscan = 0;
-- begin-expected
-- columns: enable_seqscan:text
-- row: off
-- rowcount: 1
-- end-expected
SHOW enable_seqscan;
-- begin-expected
-- columns: current_setting:text
-- row: off
-- rowcount: 1
-- end-expected
SELECT current_setting('enable_seqscan');
-- begin-expected
-- columns: setting:text
-- row: off
-- rowcount: 1
-- end-expected
SELECT setting FROM pg_settings WHERE name='enable_seqscan';
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = 'a, b';
-- begin-expected
-- columns: search_path:text
-- row: "a, b"
-- rowcount: 1
-- end-expected
SHOW search_path;
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = "MiXeD";
-- begin-expected
-- columns: search_path:text
-- row: "MiXeD"
-- rowcount: 1
-- end-expected
SHOW search_path;
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = "$user", public;
-- begin-expected
-- columns: search_path:text
-- row: "$user", public
-- rowcount: 1
-- end-expected
SHOW search_path;
-- begin-expected
-- columns: set_config:text
-- row: off
-- rowcount: 1
-- end-expected
SELECT set_config('enable_seqscan', '0', false);
-- begin-expected
-- columns: set_config:text
-- row: on
-- rowcount: 1
-- end-expected
SELECT set_config('enable_seqscan', 'yes', false);
-- begin-expected
-- ok: 0
-- end-expected
SET "zz_x.Mixed" = 'x';
-- begin-expected
-- columns: zz_x.Mixed:text
-- row: x
-- rowcount: 1
-- end-expected
SHOW "zz_x.Mixed";
-- begin-expected
-- columns: zz_x.Mixed:text
-- row: x
-- rowcount: 1
-- end-expected
SHOW "zz_x.mixed";
