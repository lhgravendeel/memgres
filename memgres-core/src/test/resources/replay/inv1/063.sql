-- source: investigation.md
-- finding: 63
-- title: The `rpad` hang is reachable from any computed empty string ⚠️ critical
-- begin-expected
-- columns: rpad:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT rpad('abc', 10, substr('a', 2));
-- PG: abc | mg: never returns
-- begin-expected
-- columns: rpad:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT rpad('abc', 10, '' || '');
-- PG: abc | mg: never returns
-- begin-expected
-- columns: rpad:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT rpad('abc', 10, repeat('z', 0));
-- PG: abc | mg: never returns
-- begin-expected
-- columns: rpad:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT rpad('abc', 10, left('x', 0));
-- PG: abc | mg: never returns;
