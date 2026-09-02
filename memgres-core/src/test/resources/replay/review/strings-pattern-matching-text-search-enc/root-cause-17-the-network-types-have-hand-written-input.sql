-- source: review-2026-08.md
-- finding: Root cause 17: the network types have hand-written input parsers that diverge from PostgreSQL's
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 17: the network types have hand-written input parsers that diverge from PostgreSQL's
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
-- begin-expected
-- columns: set_masklen:inet
-- row: 192.168.1.5
-- rowcount: 1
-- end-expected
SELECT set_masklen('192.168.1.5/24'::inet, -1);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot AND inet values of different sizes
-- end-expected-error
SELECT '192.168.1.5'::inet & '::1'::inet;
