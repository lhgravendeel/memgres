-- SQL verification for window frame and PL/pgSQL polish (M2, M3, M4, M26, M27)

CREATE TABLE v18_w (id int, val int);
INSERT INTO v18_w VALUES (1,10),(2,20),(3,30),(4,40),(5,50);

-- M2: first_value with EXCLUDE CURRENT ROW
SELECT first_value(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM v18_w ORDER BY id;
-- expected first row: 20 (current row 10 excluded, next = 20)

-- M2: last_value with EXCLUDE CURRENT ROW
SELECT last_value(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM v18_w ORDER BY id;
-- expected last row: 40 (current row 50 excluded)

-- M26: nth_value error for 0
SELECT nth_value(val, 0) OVER () FROM v18_w; -- expected-error: 22016

-- M27: RANGE interval offset
CREATE TABLE v18_d (id int, d date, val int);
INSERT INTO v18_d VALUES (1, '2024-01-01', 10), (2, '2024-01-02', 20), (3, '2024-01-04', 30);
SELECT sum(val) OVER (ORDER BY d RANGE BETWEEN '1 day'::interval PRECEDING AND CURRENT ROW) FROM v18_d ORDER BY d;
-- expected: 10, 30, 30

DROP TABLE v18_w;
DROP TABLE v18_d;
