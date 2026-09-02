-- source: investigation-2026-08.md
-- finding: 380
-- title: Window-function and partition-bound arguments are narrowed instead of type-resolved: requireIntegralArgument only rejects BigDecimal/Double/Float before an unco
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_a (id int, v int);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_vf2_a VALUES (1,10),(2,20),(3,30),(4,40);
-- begin-expected
-- columns: id:int4 | lag:int4
-- row: 1 | NULL
-- row: 2 | 10
-- row: 3 | 20
-- row: 4 | 30
-- rowcount: 4
-- end-expected
SELECT id, lag(v,'1') OVER (ORDER BY id) FROM zz_vf2_a ORDER BY id;
-- begin-expected-error
-- sqlstate: 22013
-- message-like: frame starting offset must not be negative
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY id ROWS '-1' PRECEDING) FROM zz_vf2_a ORDER BY id;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_d" does not exist
-- end-expected-error
SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '-1 day' PRECEDING AND CURRENT ROW) FROM zz_vf2_d ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ntile(numeric) does not exist
-- end-expected-error
SELECT id, ntile(2.5) OVER (ORDER BY id) FROM zz_vf2_a ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ntile(bigint) does not exist
-- end-expected-error
SELECT id, ntile(2147483648) OVER (ORDER BY id) FROM zz_vf2_a ORDER BY id;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nth_value(integer, bigint) does not exist
-- end-expected-error
SELECT id, nth_value(v, 3000000000) OVER (ORDER BY id) FROM zz_vf2_a ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ht (id int) PARTITION BY HASH (id);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "99999999999"
-- end-expected-error
CREATE TABLE zz_vf2_ht1 PARTITION OF zz_vf2_ht FOR VALUES WITH (MODULUS 99999999999, REMAINDER 0);
