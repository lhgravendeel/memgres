-- source: investigation-2026-08.md
-- finding: 382
-- title: The binary result path is only half implemented. writeBinaryValue's switch covers about twenty types and everything else falls into writeTextFallback, which wri
-- Parse 'SELECT ...'; Bind with resultFormats = [1] (binary); Describe P; Execute
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
-- columns: inet:inet
-- row: 192.168.1.1
-- rowcount: 1
-- end-expected
SELECT '192.168.1.1'::inet;
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
-- columns: circle:circle
-- row: <(0,0),5>
-- rowcount: 1
-- end-expected
SELECT '<(0,0),5>'::circle;
-- begin-expected
-- columns: array:_int4
-- row: {{1,2},{3,4}}
-- rowcount: 1
-- end-expected
SELECT ARRAY[[1,2],[3,4]]::int4[];
-- Parse 'SELECT 1::int4, ''a''::text'; Bind resultFormats=[1] (and [1,0]); Describe Portal;
