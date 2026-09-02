-- source: review-2026-08.md
-- finding: Root cause 5: RESET ALL clears the set that remembers a custom GUC exists
-- area: LISTEN/NOTIFY, advisory locks and DISCARD
-- title: Root cause 5: RESET ALL clears the set that remembers a custom GUC exists
-- begin-expected
-- ok: 0
-- end-expected
SET zz_x.k = 'v';
-- begin-expected
-- ok: 0
-- end-expected
RESET ALL;
-- begin-expected
-- columns: v:text | isnull:bool
-- row:  | f
-- rowcount: 1
-- end-expected
SELECT current_setting('zz_x.k', true) AS v, current_setting('zz_x.k', true) IS NULL AS isnull;
