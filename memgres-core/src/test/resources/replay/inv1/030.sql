-- source: investigation.md
-- finding: 30
-- title: `string_to_array` with an empty delimiter
-- begin-expected
-- columns: array_length:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT array_length(string_to_array('abc', ''), 1);
-- PG: 1   memgres: 4;
