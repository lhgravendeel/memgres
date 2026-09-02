-- source: review-2026-08.md
-- finding: NULL short-circuits before three-valued logic is applied
-- area: Aggregates, window functions and grouping
-- title: NULL short-circuits before three-valued logic is applied
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 1 BETWEEN 2 AND NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 3 BETWEEN NULL AND 2;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 1 NOT BETWEEN 2 AND NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT ROW(1,NULL) IN (ROW(1,2));
-- begin-expected
-- columns: ?column?:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT ROW(1,NULL) IN (ROW(1,2), ROW(1,NULL));
