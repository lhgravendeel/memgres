-- source: investigation.md
-- finding: 60
-- title: Degenerate arguments to string and set-returning functions (11 cases)
-- unrunnable: the report wrote this reproducer abbreviated
SELECT replace('abc', '', 'X');
-- PG: abc  | mg: XaXbXcX
SELECT string_agg(v, NULL) FROM …;
-- PG: ab   | mg: a,b     ← NULL separator becomes a comma
SELECT to_char(1, '');
-- PG: ''   | mg: ' 1'
SELECT to_char(now(), '') IS NOT NULL;
-- PG: false| mg: true
SELECT round(1.5, -1)::text;
-- PG: 0    | mg: 0E+1    ← BigDecimal scientific notation
SELECT trunc(1.5, -1)::text;
-- PG: 0    | mg: 0E+1
SELECT count(*) FROM generate_subscripts(ARRAY[1,2], 5);
-- PG: 0 | mg: 2
SELECT array_length(regexp_matches('abc', 'x*', 'g'), 1);
-- PG: 1 | mg: NULL;;
