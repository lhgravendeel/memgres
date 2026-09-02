-- source: review-2026-08.md
-- finding: Aggregate calls have no signature resolution
-- area: Aggregates, window functions and grouping
-- title: Aggregate calls have no signature resolution
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(uuid) does not exist
-- end-expected-error
SELECT max(x) FROM (VALUES ('00000000-0000-0000-0000-000000000001'::uuid)) t(x);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(boolean) does not exist
-- end-expected-error
SELECT max(x) FROM (VALUES (true)) t(x);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(json) does not exist
-- end-expected-error
SELECT max(x) FROM (VALUES ('{"a":1}'::json)) t(x);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(int4range) does not exist
-- end-expected-error
SELECT max(x) FROM (VALUES ('[1,3)'::int4range)) t(x);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(xml) does not exist
-- end-expected-error
SELECT max(x) FROM (VALUES ('<a/>'::xml)) t(x);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum() does not exist
-- end-expected-error
SELECT sum(*) FROM (VALUES (1)) t(v);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: count(*) must be used to call a parameterless aggregate function
-- end-expected-error
SELECT count() FROM (VALUES (1)) t(v);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "*"
-- end-expected-error
SELECT count(DISTINCT *) FROM (VALUES (1),(2)) t(v);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
SELECT grouping() FROM (VALUES (1),(2)) t(v) GROUP BY v ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "OVER"
-- end-expected-error
SELECT grouping(v) OVER () FROM (VALUES (1),(2)) t(v) GROUP BY v ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FILTER"
-- end-expected-error
SELECT grouping(v) FILTER (WHERE true) FROM (VALUES (1),(2)) t(v) GROUP BY v ORDER BY 1;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "zz"
-- end-expected-error
SELECT rank('zz') WITHIN GROUP (ORDER BY v) FROM (VALUES (10),(20),(30)) t(v);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "zz"
-- end-expected-error
SELECT lag(v,1,'zz') OVER (ORDER BY v) FROM (VALUES (10),(20),(30)) t(v) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: RANGE with offset PRECEDING/FOLLOWING is not supported for column type integer and offset type text
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN '1'::text PRECEDING AND CURRENT ROW)
  FROM (VALUES (10),(20),(30)) t(v) ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_a (id int, g int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_a VALUES (1,1,10),(2,1,20),(3,2,30);
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "zz_vf_a.v" must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT percentile_cont(v/100.0) WITHIN GROUP (ORDER BY v) FROM zz_vf_a;
