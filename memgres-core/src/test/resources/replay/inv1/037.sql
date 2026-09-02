-- source: investigation.md
-- finding: 37
-- title: Set-operation `ORDER BY` is unvalidated
-- unrunnable: the report wrote this reproducer abbreviated
SELECT 1 UNION SELECT 2 ORDER BY 5;
-- PG: 42P10 position 5 not in select list | mg: OK
… UNION SELECT 2 ORDER BY a + 1;
-- PG: 0A000 invalid UNION ORDER BY clause | mg: OK
… UNION ALL SELECT 2 ORDER BY t.a;
-- PG: 42P01 missing FROM-clause entry      | mg: OK
… UNION ALL SELECT 2 ORDER BY 1 OFFSET -1;
-- PG: 2201X OFFSET must not be negative    | mg: OK
SELECT a AS x FROM … UNION ALL SELECT 2 ORDER BY a;
-- PG: 42703 column "a" does not exist   | mg: OK;;
