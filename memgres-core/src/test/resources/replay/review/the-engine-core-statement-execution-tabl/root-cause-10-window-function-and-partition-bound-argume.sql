-- source: review-2026-08.md
-- finding: Root cause 10: window-function and partition-bound arguments are narrowed, not type-resolved
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 10: window-function and partition-bound arguments are narrowed, not type-resolved
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_a (id int, v int);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_vf2_a VALUES (1,10),(2,20),(3,30),(4,40);
-- begin-expected
-- columns: id:int4 | lg:int4
-- row: 1 | NULL
-- row: 2 | 10
-- row: 3 | 20
-- row: 4 | 30
-- rowcount: 4
-- end-expected
SELECT id, lag(v,'1') OVER (ORDER BY id) AS lg FROM zz_vf2_a ORDER BY id;
-- begin-expected
-- columns: id:int4 | ld:int4
-- row: 1 | 20
-- row: 2 | 30
-- row: 3 | 40
-- row: 4 | NULL
-- rowcount: 4
-- end-expected
SELECT id, lead(v,'1') OVER (ORDER BY id) AS ld FROM zz_vf2_a ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- message-like: frame starting offset must not be negative
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS '-1' PRECEDING) AS c FROM zz_vf2_a ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_d (id int, d date);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_d VALUES (1,DATE '2024-01-01'),(2,DATE '2024-01-02');
-- begin-expected-error
-- sqlstate: 22013
-- message-like: invalid preceding or following size in window function
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '-1 day' PRECEDING AND CURRENT ROW) AS c FROM zz_vf2_d ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- message-like: invalid preceding or following size in window function
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN CURRENT ROW AND INTERVAL '-1 day' FOLLOWING) AS c FROM zz_vf2_d ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ntile(numeric) does not exist
-- end-expected-error
SELECT id, ntile(2.5) OVER (ORDER BY id) AS nt FROM zz_vf2_a ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ntile(bigint) does not exist
-- end-expected-error
SELECT id, ntile(2147483648) OVER (ORDER BY id) AS nt FROM zz_vf2_a ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nth_value(integer, bigint) does not exist
-- end-expected-error
SELECT id, nth_value(v, 3000000000) OVER (ORDER BY id) AS nv FROM zz_vf2_a ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ht (id int) PARTITION BY HASH (id);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "99999999999"
-- end-expected-error
CREATE TABLE zz_vf2_ht1 PARTITION OF zz_vf2_ht FOR VALUES WITH (MODULUS 99999999999, REMAINDER 0);
