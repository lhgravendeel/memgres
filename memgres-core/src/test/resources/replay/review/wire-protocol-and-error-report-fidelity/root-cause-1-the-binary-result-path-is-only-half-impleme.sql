-- source: review-2026-08.md
-- finding: Root cause 1: the binary result path is only half implemented
-- area: Wire protocol and error-report fidelity
-- title: Root cause 1: the binary result path is only half implemented
-- Bind with resultFormats = [1], then Execute
-- begin-expected
-- columns: interval:interval
-- row: 1 year 2 mons 3 days 04:05:06
-- rowcount: 1
-- end-expected
SELECT '1 year 2 mons 3 days 04:05:06'::interval;
-- begin-expected
-- columns: jsonb:jsonb
-- row: {"a": 1}
-- rowcount: 1
-- end-expected
SELECT '{"a": 1}'::jsonb;
-- begin-expected
-- columns: bit:bit
-- row: 101
-- rowcount: 1
-- end-expected
SELECT '101'::bit(3);
-- begin-expected
-- columns: int4range:int4range
-- row: [1,5)
-- rowcount: 1
-- end-expected
SELECT '[1,5)'::int4range;
-- begin-expected
-- columns: array:_int4
-- row: {{1,2},{3,4}}
-- rowcount: 1
-- end-expected
SELECT ARRAY[[1,2],[3,4]]::int4[];
-- Parse 'SELECT 1::int4, ''a''::text'; Bind resultFormats=[1]; Describe Portal;
