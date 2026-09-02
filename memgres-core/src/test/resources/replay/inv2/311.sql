-- source: investigation-2026-08.md
-- finding: 311
-- title: A GUC value is stored as the text that was written: set() lower-cases exactly six spellings and canonicalValue has an enum arm and no bool, list or quoting arm,
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
