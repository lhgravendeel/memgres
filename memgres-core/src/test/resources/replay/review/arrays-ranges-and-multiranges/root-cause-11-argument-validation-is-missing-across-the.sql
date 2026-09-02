-- source: review-2026-08.md
-- finding: Root cause 11: argument validation is missing across the element/index surface
-- area: Arrays, ranges and multiranges
-- title: Root cause 11: argument validation is missing across the element/index surface
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
-- sqlstate: 22000
-- message-like: argument must be empty or one-dimensional array
-- end-expected-error
SELECT array_append(ARRAY[[1,2],[3,4]], 5);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: sample size must be between 0 and 3
-- end-expected-error
SELECT array_sample(ARRAY[1,2,3], 5) IS NOT NULL;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "3.7"
-- end-expected-error
SELECT array_append(ARRAY[1,2], '3.7');
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: bit index 5 out of valid range (0..4)
-- end-expected-error
SELECT get_bit(B'10001', 5);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: new bit must be 0 or 1
-- end-expected-error
SELECT set_bit(B'10001', 1, 2);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function hstore(text[], text[]) does not exist
-- end-expected-error
SELECT hstore(ARRAY['a','b'], ARRAY['1'])::text;
-- begin-expected-error
-- sqlstate: 2200M
-- message-like: could not parse XML document
-- end-expected-error
SELECT xpath('/a', 'not xml');
-- begin-expected
-- columns: ?column?:bool | ?column?:bool | ?column?:bool | ?column?:bool
-- row: f | f | f | f
-- rowcount: 1
-- end-expected
SELECT '[1,2' IS JSON, '{"a":1' IS JSON, '[1,,2]' IS JSON, '{"a":1}{"b":2}' IS JSON;
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
-- sqlstate: 42883
-- message-like: function make_date(bigint, integer, integer) does not exist
-- end-expected-error
SELECT make_date(4294967297, 1, 1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_si" does not exist
-- end-expected-error
INSERT INTO zz_vf_si VALUES (1, 1.6), (2, 2.5), (3, -1.6);
-- s smallint
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_si" does not exist
-- end-expected-error
INSERT INTO zz_vf_si VALUES (4, 32767.6);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: "char" out of range
-- end-expected-error
SELECT 300::"char";
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
DO $$ begin for i in 1..3000000000 loop exit; end loop; end $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
DO $$ declare x int; begin x := 1 end $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ";"
-- end-expected-error
DO $$ begin if true then null; end; end $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ";"
-- end-expected-error
DO $$ begin raise notice; end $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: "nosuchcursor" is not a known variable
-- end-expected-error
DO $$ begin open nosuchcursor; end $$;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_pr" does not exist
-- end-expected-error
CREATE TABLE zz_vf_pr1 PARTITION OF zz_vf_pr FOR VALUES FROM (MINVALUE, 5) TO (0, 0);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized headline parameter: "BadOption"
-- end-expected-error
SELECT ts_headline('english','the quick brown fox',to_tsquery('english','fox'),'BadOption=1');
