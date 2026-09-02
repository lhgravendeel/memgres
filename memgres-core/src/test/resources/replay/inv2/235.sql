-- source: investigation-2026-08.md
-- finding: 235
-- title: GucSettings.resetAll() clears customPlaceholders as well as sessionOverrides, so RESET ALL and DISCARD ALL forget that a custom GUC was ever created; the single
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
