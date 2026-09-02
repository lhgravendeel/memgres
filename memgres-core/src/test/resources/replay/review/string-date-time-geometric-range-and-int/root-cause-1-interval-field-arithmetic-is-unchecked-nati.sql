-- source: review-2026-08.md
-- finding: Root cause 1: interval field arithmetic is unchecked native-width Java arithmetic, and Integer.MAX_VALUE months doubles as the infinity sentinel
-- area: String, date/time, geometric, range and interval implementations
-- title: Root cause 1: interval field arithmetic is unchecked native-width Java arithmetic, and Integer.MAX_VALUE months doubles as the infinity sentinel
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT make_interval(years => 200000000);
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT make_interval(weeks => 400000000);
-- begin-expected
-- columns: make_interval:interval
-- row: 178956970 years 7 mons
-- rowcount: 1
-- end-expected
SELECT make_interval(months => 2147483647);
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT make_interval(years => 178956970, months => 8);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_iv (id int, x interval);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_iv VALUES (1, interval '2000000000 mons'), (2, interval '1000000000 mons'), (3, interval '1 day');
-- begin-expected
-- columns: id:int4
-- row: 3
-- row: 2
-- row: 1
-- rowcount: 3
-- end-expected
SELECT id FROM zz_vf2_iv ORDER BY x, id;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT interval '1000000000 mons' > interval '1 day';
