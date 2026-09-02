-- source: investigation.md
-- finding: 82
-- title: `LIMIT`/`OFFSET` truncate to 32 bits ⚠️
-- unrunnable: the report wrote this reproducer abbreviated
SELECT … LIMIT 2147483648;
-- PG: returns all rows | mg: 2201W LIMIT must not be negative
SELECT … LIMIT 9223372036854775807;
-- PG: all rows          | mg: 2201W
SELECT … OFFSET 2147483648;
-- PG: 0 rows            | mg: 2201X OFFSET must not be negative
SELECT left('abcde', 4294967296);
-- PG: 42883 function left(unknown, bigint) does not exist | mg: ''
SELECT repeat('ab', 4294967296);
-- PG: 42883 | mg: ''
SELECT lpad('abc', 4294967296, 'x');
-- PG: 42883 | mg: '';;
