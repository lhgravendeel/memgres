-- source: review-2026-08.md
-- finding: Root cause 12: unguarded Java calls escape as XX000
-- area: Arrays, ranges and multiranges
-- title: Root cause 12: unguarded Java calls escape as XX000
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: wrong number of array subscripts
-- end-expected-error
SELECT array_fill(1, ARRAY[[2]]);
-- begin-expected
-- columns: regexp_instr:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT regexp_instr('banana', '(a)(n)', 1, 1, 0, '', 5);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_vf_e1" does not exist
-- end-expected-error
SELECT enum_range('c'::zz_vf_e1, 'a'::zz_vf_e1);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: could not determine polymorphic type because input has type unknown
-- end-expected-error
SELECT * FROM generate_subscripts('[a:b]={1,2}', 1);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value "99999999999999999999" is out of range for type bigint
-- end-expected-error
CREATE SEQUENCE zz_vf_sq START 99999999999999999999;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value "99999999999" is out of range for type integer
-- end-expected-error
SELECT 1::numeric(99999999999);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value "99999999999" is out of range for type integer
-- end-expected-error
CREATE TABLE zz_vf_num (a numeric(99999999999));
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT ts_headline('english','the quick brown fox',to_tsquery('english','fox'),'MaxWords=abc');
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date field value out of range: 2020-13-01
-- end-expected-error
SELECT make_timestamp(2020, 13, 1, 0, 0, 0);
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date field value out of range: 0-01-01
-- end-expected-error
SELECT make_date(0,1,1);
-- begin-expected
-- columns: to_timestamp:timestamptz
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT to_timestamp('infinity'::float8);
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp cannot be NaN
-- end-expected-error
SELECT to_timestamp('nan'::float8);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: trailing junk after numeric literal at or near "1e"
-- end-expected-error
SELECT 1e;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid Unicode escape value at or near "\U00110000"
-- end-expected-error
SELECT E'\U00110000';
-- begin-expected
-- columns: f1:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT (ROW(1,2)).f1;
