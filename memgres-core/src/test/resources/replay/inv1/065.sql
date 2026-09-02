-- source: investigation.md
-- finding: 65
-- title: NULL in a fill, delimiter or pattern argument ⚠️ (7 cases)
-- begin-expected
-- columns: rpad:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT rpad('abc', 10, NULL);
-- PG: NULL | mg: 'abcnullnul'
-- begin-expected
-- columns: lpad:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT lpad('abc', 10, NULL);
-- PG: NULL | mg: 'nullnulabc'
-- begin-expected
-- columns: array_to_string:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT array_to_string(ARRAY[1,2], NULL);
-- PG: NULL | mg: '1null2'
-- begin-expected
-- columns: replace:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT replace('abc', NULL, 'x');
-- PG: NULL | mg: XX000 Cannot invoke "Object.toString()"
-- begin-expected
-- columns: replace:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT replace('abc', 'b', NULL);
-- PG: NULL | mg: XX000
-- begin-expected
-- columns: translate:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT translate('abc', NULL, 'x');
-- PG: NULL | mg: XX000
-- begin-expected
-- columns: split_part:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT split_part('a,b', NULL, 1);
-- PG: NULL | mg: XX000;
