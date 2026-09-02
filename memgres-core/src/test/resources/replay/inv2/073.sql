-- source: investigation-2026-08.md
-- finding: 73
-- title: The network types have hand-written input parsers that diverge from PostgreSQL's in both directions (abbreviated-with-mask rejected, surrounding whitespace acce
-- begin-expected
-- columns: inet:inet
-- row: 10.0.0.0/8
-- rowcount: 1
-- end-expected
SELECT '10/8'::inet;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type inet: "192.168.1.5 "
-- end-expected-error
SELECT '192.168.1.5 '::inet;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type inet: " 192.168.1.5"
-- end-expected-error
SELECT ' 192.168.1.5'::inet;
-- begin-expected
-- columns: set_masklen:inet
-- row: 192.168.1.5
-- rowcount: 1
-- end-expected
SELECT set_masklen('192.168.1.5/24'::inet, -1);
-- begin-expected
-- columns: masklen:int4
-- row: 32
-- rowcount: 1
-- end-expected
SELECT masklen(set_masklen('192.168.1.5/24'::inet, -1));
-- begin-expected
-- columns: macaddr:macaddr
-- row: 08:00:2b:01:02:03
-- rowcount: 1
-- end-expected
SELECT '0800-2b01-0203'::macaddr;
-- begin-expected
-- columns: macaddr8:macaddr8
-- row: 08:00:2b:01:02:03:04:05
-- rowcount: 1
-- end-expected
SELECT '08002b:0102030405'::macaddr8;
-- begin-expected
-- columns: macaddr8:macaddr8
-- row: 08:00:2b:01:02:03:04:05
-- rowcount: 1
-- end-expected
SELECT '08002b-0102030405'::macaddr8;
-- begin-expected
-- columns: macaddr8:macaddr8
-- row: 08:00:2b:01:02:03:04:05
-- rowcount: 1
-- end-expected
SELECT '0800-2b01-0203-0405'::macaddr8;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot AND inet values of different sizes
-- end-expected-error
SELECT '192.168.1.5'::inet & '::1'::inet;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot subtract inet values of different sizes
-- end-expected-error
SELECT '192.168.1.5'::inet - '::1'::inet;
