-- source: investigation-2026-08.md
-- finding: 86
-- title: Unrelated singletons in this area
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
-- begin-expected
-- columns: string_to_array:_text
-- row: {a,NULL,c}
-- rowcount: 1
-- end-expected
SELECT string_to_array('abc', NULL, 'b');
-- begin-expected
-- columns: string_to_array:_text
-- row: {a,NULL,c}
-- rowcount: 1
-- end-expected
SELECT string_to_array('a.b.c', '.', 'b');
-- begin-expected
-- columns: extract:numeric
-- row: -1
-- rowcount: 1
-- end-expected
SELECT extract(decade from date '0010-01-01 BC');
-- begin-expected
-- columns: extract:numeric
-- row: -10
-- rowcount: 1
-- end-expected
SELECT extract(decade from date '0100-01-01 BC');
-- begin-expected
-- columns: date_part:float8
-- row: -1
-- rowcount: 1
-- end-expected
SELECT date_part('decade', date '0010-01-01 BC');
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 'UTC';
-- begin-expected
-- columns: pg_typeof:text
-- row: timestamp with time zone
-- rowcount: 1
-- end-expected
SELECT pg_typeof(date_trunc('day', TIMESTAMP '2020-05-15 10:00:00', 'UTC'))::text;
-- begin-expected
-- columns: date_trunc:text
-- row: 2020-05-14 15:00:00+00
-- rowcount: 1
-- end-expected
SELECT date_trunc('day', TIMESTAMP '2020-05-15 10:00:00', 'Asia/Tokyo')::text;
-- begin-expected
-- columns: substring:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT substring('abcdef', '2', '3');
-- begin-expected
-- columns: btrim:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT trim('xabcx', 'x');
