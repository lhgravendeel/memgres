-- source: investigation-2026-08.md
-- finding: 22
-- title: Argument validation is missing across the element/index/option surface: sample sizes, bit indexes, array dimensionality, hstore bounds, headline option names, P
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "3.7"
-- end-expected-error
SELECT array_append(ARRAY[1,2], '3.7');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function make_date(bigint, integer, integer) does not exist
-- end-expected-error
SELECT make_date(4294967297, 1, 1);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function make_time(bigint, integer, integer) does not exist
-- end-expected-error
SELECT make_time(4294967304, 0, 0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_si (id int, s smallint);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_si VALUES (1, 1.6), (2, 2.5), (3, -1.6);
-- begin-expected
-- columns: id:int4 | s:int2
-- row: 1 | 2
-- row: 2 | 3
-- row: 3 | -2
-- rowcount: 3
-- end-expected
SELECT id, s FROM zz_vf_si ORDER BY id;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
INSERT INTO zz_vf_si VALUES (4, 32767.6);
-- begin-expected
-- columns: array_replace:_int4
-- row: {{1,2},{9,4}}
-- rowcount: 1
-- end-expected
SELECT array_replace(ARRAY[[1,2],[3,4]], 3, 9);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: searching for elements in multidimensional arrays is not supported
-- end-expected-error
SELECT array_position(ARRAY[[1,2],[3,4]], 3);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: removing elements from multidimensional arrays is not supported
-- end-expected-error
SELECT array_remove(ARRAY[[1,2],[3,4]], 3);
-- begin-expected-error
-- sqlstate: 22000
-- message-like: argument must be empty or one-dimensional array
-- end-expected-error
SELECT array_append(ARRAY[[1,2],[3,4]], 5);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[1,2' IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '{"a":1' IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[1,,2]' IS JSON;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '{"a":1}{"b":2}' IS JSON;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: step size cannot be infinite
-- end-expected-error
SELECT * FROM generate_series('2020-01-01'::timestamp,'2020-01-05'::timestamp,'infinity'::interval);
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
SELECT * FROM generate_series('2020-01-01'::timestamp,'infinity'::timestamp, interval '1 day') LIMIT 2;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: "char" out of range
-- end-expected-error
SELECT 300::"char";
