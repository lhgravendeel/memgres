-- source: review-2026-08.md
-- finding: Root cause 5: strict functions null-check the argument they think of as the value and coerce or stringify the rest
-- area: String, date/time, geometric, range and interval implementations
-- title: Root cause 5: strict functions null-check the argument they think of as the value and coerce or stringify the rest
-- begin-expected
-- columns: make_date:date
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT make_date(NULL, 1, 1);
-- begin-expected
-- columns: make_date:date
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT make_date(2020, NULL, 1);
-- begin-expected
-- columns: make_interval:interval
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT make_interval(NULL);
-- begin-expected
-- columns: get_byte:int4
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT get_byte('\x0102'::bytea, NULL);
-- begin-expected
-- columns: get_bit:int4
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT get_bit('\xff'::bytea, NULL);
-- begin-expected
-- columns: set_byte:bytea
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT set_byte('\x0102'::bytea, NULL, 9);
-- begin-expected
-- columns: to_number:numeric
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT to_number('123', NULL);
-- begin-expected
-- columns: encode:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT encode('\x0102'::bytea, NULL);
-- begin-expected
-- columns: decode:bytea
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT decode('abcd', NULL);
-- begin-expected
-- columns: make_timestamptz:timestamptz
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT make_timestamptz(2020, 1, 1, 0, 0, 0, NULL);
