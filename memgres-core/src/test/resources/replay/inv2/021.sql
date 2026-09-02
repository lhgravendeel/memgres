-- source: investigation-2026-08.md
-- finding: 21
-- title: Unguarded Integer.parseInt / Long.parseLong / Matcher.group / List.subList / Instant conversions let java.lang exceptions escape as XX000 'Internal error', whic
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
-- begin-expected
-- columns: regexp_substr:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT regexp_substr('abc', '(a)', 1, 1, '', 5);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_e1 AS ENUM ('a','b','c');
-- begin-expected
-- columns: enum_range:_zz_vf_e1
-- row: {}
-- rowcount: 1
-- end-expected
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
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT ts_headline('english','the quick brown fox',to_tsquery('english','fox'),'MaxWords=abc');
