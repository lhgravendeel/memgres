-- source: investigation.md
-- finding: 24
-- title: `rpad` with an empty fill string never terminates ⚠️ critical
-- begin-expected
-- columns: rpad:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT rpad('abc', 10, '');
-- PG: 'abc'   memgres: never returns;
