-- The type a window call answers in, and the names PostgreSQL uses when an aggregate has no
-- overload for what it was handed.
--
-- A window call's value was threaded back through the expression machinery as a string literal,
-- which is PostgreSQL's "unknown": pg_typeof(sum(x) OVER ()) answered unknown where PostgreSQL
-- answers numeric, the result column was described as text, and anything computed from one
-- resolved against text -- sum(sum(total)) OVER () + 1 was described as an integer, so a client
-- reading it as one saw 256 where PostgreSQL has 256.75. The value now carries the type its own
-- expression has, and the description and the arithmetic both follow from that. Allowing an
-- aggregate under a window function is what branch 8 opened, so this is that path being typed.
--
-- Four things measured against PostgreSQL 18 in the same neighbourhood:
--
--  1. The type in "function sum(...) does not exist" was the catalog's spelling -- varchar,
--     bpchar, bool -- where PostgreSQL writes the SQL one: character varying, character,
--     boolean. avg over one of those did not reach the message at all and failed as XX000.
--  2. A RANGE frame offset of INTERVAL '1 month' over a date column counted thirty days rather
--     than a calendar month, so the frame started a day late.
--  3. GROUPING over a select-list alias reported a misplaced GROUPING (42803) where PostgreSQL
--     reports the undefined column (42703) it resolves first.
--  4. HAVING x IN (SELECT ...) read the subquery as a single value, so a second row raised
--     21000 instead of the membership test answering.
--
-- Every statement that returns rows sorts them: PostgreSQL's hash aggregate has no order of its
-- own.

-- setup
DROP VIEW IF EXISTS art_v CASCADE;
DROP TABLE IF EXISTS art_ord CASCADE;
DROP TABLE IF EXISTS art_t2 CASCADE;
DROP TABLE IF EXISTS art_d CASCADE;
DROP TABLE IF EXISTS art_ev CASCADE;
DROP TABLE IF EXISTS art_cust CASCADE;

CREATE TABLE art_ord (id int PRIMARY KEY, cust_id int, total numeric(10,2));
INSERT INTO art_ord VALUES (10,1,100.00),(11,1,50.50),(12,2,75.25),(13,3,10.00),(14,3,20.00);

CREATE TABLE art_t2 (id int PRIMARY KEY, s varchar(20), c char(5), b boolean);
INSERT INTO art_t2 VALUES (1,'a','b',true),(2,'c','d',false);

CREATE TABLE art_d (id int PRIMARY KEY, s text);
INSERT INTO art_d VALUES (1,'x'),(2,'y');

CREATE TABLE art_ev (id int PRIMARY KEY, d date, amt int);
INSERT INTO art_ev VALUES (1,DATE '2024-01-01',10),(2,DATE '2024-01-03',20),
                          (3,DATE '2024-01-10',30),(4,DATE '2024-02-01',40);

CREATE TABLE art_cust (id int PRIMARY KEY, name text, region text, active boolean);
INSERT INTO art_cust VALUES (1,'Ann','EU',true),(2,'Bob','US',false),
                            (3,'Cid','EU',true),(4,'Dee','APAC',true);

CREATE VIEW art_v AS SELECT cust_id, sum(total) s FROM art_ord GROUP BY cust_id;

-- 1: a window over an aggregate answers in the aggregate's type

-- begin-expected
-- columns: cust_id | pg_typeof
-- row: 1, numeric
-- row: 2, numeric
-- row: 3, numeric
-- end-expected
SELECT cust_id, pg_typeof(sum(sum(total)) OVER ()) FROM art_ord GROUP BY cust_id ORDER BY 1;

-- begin-expected
-- columns: cust_id | pg_typeof
-- row: 1, numeric
-- row: 2, numeric
-- row: 3, numeric
-- end-expected
SELECT cust_id, pg_typeof(max(sum(total)) OVER ()) FROM art_ord GROUP BY cust_id ORDER BY 1;

-- begin-expected
-- columns: cust_id | pg_typeof
-- row: 1, numeric
-- row: 2, numeric
-- row: 3, numeric
-- end-expected
SELECT cust_id, pg_typeof(avg(sum(total)) OVER ()) FROM art_ord GROUP BY cust_id ORDER BY 1;

-- sum over a count is sum(bigint), which is numeric and not another bigint

-- begin-expected
-- columns: cust_id | pg_typeof
-- row: 1, numeric
-- row: 2, numeric
-- row: 3, numeric
-- end-expected
SELECT cust_id, pg_typeof(sum(count(*)) OVER ()) FROM art_ord GROUP BY cust_id ORDER BY 1;

-- 2: a plain window call carries its own type too

-- begin-expected
-- columns: pg_typeof | pg_typeof | pg_typeof | pg_typeof
-- row: numeric, bigint, numeric, bigint
-- end-expected
SELECT DISTINCT pg_typeof(sum(total) OVER ()), pg_typeof(sum(id) OVER ()),
       pg_typeof(avg(id) OVER ()), pg_typeof(count(*) OVER ()) FROM art_ord;

-- begin-expected
-- columns: pg_typeof | pg_typeof | pg_typeof
-- row: bigint, bigint, numeric
-- end-expected
SELECT DISTINCT pg_typeof(row_number() OVER ()), pg_typeof(rank() OVER (ORDER BY id)),
       pg_typeof(min(total) OVER ()) FROM art_ord;

-- the value-shifting calls answer in the type of the value they shift, on every row --
-- including the first, where lag has no value at all

-- begin-expected
-- columns: id | pg_typeof
-- row: 10, numeric
-- row: 11, numeric
-- row: 12, numeric
-- row: 13, numeric
-- row: 14, numeric
-- end-expected
SELECT id, pg_typeof(lag(total) OVER (ORDER BY id)) FROM art_ord ORDER BY 1;

-- begin-expected
-- columns: id | pg_typeof
-- row: 10, numeric
-- row: 11, numeric
-- row: 12, numeric
-- row: 13, numeric
-- row: 14, numeric
-- end-expected
SELECT id, pg_typeof(first_value(total) OVER (ORDER BY id)) FROM art_ord ORDER BY 1;

-- 3: a value computed from a window result keeps its fraction

-- begin-expected
-- columns: cust_id | ?column?
-- row: 1, 256.75
-- row: 2, 256.75
-- row: 3, 256.75
-- end-expected
SELECT cust_id, sum(sum(total)) OVER () + 1 FROM art_ord GROUP BY cust_id ORDER BY 1;

-- begin-expected
-- columns: cust_id | ?column?
-- row: 1, 255.50
-- row: 2, 255.50
-- row: 3, 255.50
-- end-expected
SELECT cust_id, sum(sum(total)) OVER () - 0.25 FROM art_ord GROUP BY cust_id ORDER BY 1;

-- begin-expected
-- columns: cust_id | round
-- row: 1, 85.25
-- row: 2, 85.25
-- row: 3, 85.25
-- end-expected
SELECT cust_id, round(avg(sum(total)) OVER (), 2) FROM art_ord GROUP BY cust_id ORDER BY 1;

-- 4: a window value is still readable where it is NULL

-- begin-expected
-- columns: id | ?column?
-- row: 10, t
-- row: 11, f
-- row: 12, f
-- row: 13, f
-- row: 14, f
-- end-expected
SELECT id, lag(total) OVER (ORDER BY id) IS NULL FROM art_ord ORDER BY 1;

-- begin-expected
-- columns: id | coalesce
-- row: 10, 0
-- row: 11, 100.00
-- row: 12, 50.50
-- row: 13, 75.25
-- row: 14, 10.00
-- end-expected
SELECT id, coalesce(lag(total) OVER (ORDER BY id), 0) FROM art_ord ORDER BY 1;

-- begin-expected
-- columns: id | abs
-- row: 10, NULL
-- row: 11, 100.00
-- row: 12, 50.50
-- row: 13, 75.25
-- row: 14, 10.00
-- end-expected
SELECT id, abs(lag(total) OVER (ORDER BY id)) FROM art_ord ORDER BY 1;

-- 5: the missing aggregate names its argument the way SQL spells it

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(character varying) does not exist
-- end-expected-error
SELECT id, count(*) FROM art_t2 GROUP BY id HAVING sum(s) > 1;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(character) does not exist
-- end-expected-error
SELECT id, count(*) FROM art_t2 GROUP BY id HAVING sum(c) > 1;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function avg(boolean) does not exist
-- end-expected-error
SELECT id, count(*) FROM art_t2 GROUP BY id HAVING avg(b) > 1;

-- the same names outside HAVING, where the message used to read the value's type

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(character varying) does not exist
-- end-expected-error
SELECT sum(s) FROM art_t2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(character) does not exist
-- end-expected-error
SELECT sum(c) FROM art_t2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(boolean) does not exist
-- end-expected-error
SELECT sum(b) FROM art_t2;

-- 6: an aggregate with no overload is a missing function, not an internal error

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function avg(character varying) does not exist
-- end-expected-error
SELECT avg(s) FROM art_t2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function avg(character) does not exist
-- end-expected-error
SELECT avg(c) FROM art_t2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function avg(boolean) does not exist
-- end-expected-error
SELECT avg(b) FROM art_t2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function bit_and(character varying) does not exist
-- end-expected-error
SELECT bit_and(s) FROM art_t2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(character varying) does not exist
-- end-expected-error
SELECT sum(s) OVER () FROM art_t2;

-- 7: a RANGE frame offset is an interval, not a count of days

-- begin-expected
-- columns: id | d | count
-- row: 1, 2024-01-01, 4
-- row: 2, 2024-01-03, 4
-- row: 3, 2024-01-10, 4
-- row: 4, 2024-02-01, 4
-- end-expected
SELECT id, d, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 month' PRECEDING
       AND INTERVAL '1 month' FOLLOWING) FROM art_ev ORDER BY 1;

-- begin-expected
-- columns: id | d | count
-- row: 1, 2024-01-01, 1
-- row: 2, 2024-01-03, 1
-- row: 3, 2024-01-10, 1
-- row: 4, 2024-02-01, 1
-- end-expected
SELECT id, d, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 day' PRECEDING
       AND CURRENT ROW) FROM art_ev ORDER BY 1;

-- begin-expected
-- columns: id | d | sum
-- row: 1, 2024-01-01, 10
-- row: 2, 2024-01-03, 30
-- row: 3, 2024-01-10, 30
-- row: 4, 2024-02-01, 40
-- end-expected
SELECT id, d, sum(amt) OVER (ORDER BY d RANGE BETWEEN INTERVAL '2 days' PRECEDING
       AND CURRENT ROW) FROM art_ev ORDER BY 1;

-- a numeric offset over a numeric ordering column is untouched by that

-- begin-expected
-- columns: id | sum
-- row: 1, 10
-- row: 2, 20
-- row: 3, 30
-- row: 4, 40
-- end-expected
SELECT id, sum(amt) OVER (ORDER BY id RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING)
FROM art_ev ORDER BY 1;

-- 8: GROUPING over a name that is not a column

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "k" does not exist
-- end-expected-error
SELECT s AS k, grouping(k), count(*) FROM art_d GROUP BY ROLLUP(k);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
SELECT grouping(nosuch) FROM art_d GROUP BY ROLLUP(s);

-- a name that is a column but not one the query groups by keeps the GROUPING message

-- begin-expected-error
-- sqlstate: 42803
-- message-like: arguments to GROUPING must be grouping expressions
-- end-expected-error
SELECT s, grouping(id) FROM art_d GROUP BY ROLLUP(s);

-- and a GROUPING over a name the query does group by is simply valid

-- begin-expected
-- columns: s | grouping
-- row: x, 0
-- row: y, 0
-- row: NULL, 1
-- end-expected
SELECT s, grouping(s) FROM art_d GROUP BY ROLLUP(s) ORDER BY 2, 1;

-- begin-expected
-- columns: k | count
-- row: x, 1
-- row: y, 1
-- row: NULL, 2
-- end-expected
SELECT s AS k, count(*) FROM art_d GROUP BY ROLLUP(k) ORDER BY 2, 1;

-- 9: a subquery in HAVING is a set, not a scalar

-- begin-expected
-- columns: cust_id | sum
-- row: 1, 150.50
-- row: 2, 75.25
-- end-expected
SELECT cust_id, sum(total) FROM art_ord GROUP BY cust_id
HAVING cust_id IN (SELECT id FROM art_d) ORDER BY 1;

-- begin-expected
-- columns: cust_id | sum
-- row: 1, 150.50
-- row: 2, 75.25
-- row: 3, 30.00
-- end-expected
SELECT cust_id, sum(total) FROM art_ord GROUP BY cust_id
HAVING count(*) IN (SELECT id FROM art_d) ORDER BY 1;

-- begin-expected
-- columns: cust_id
-- row: 3
-- end-expected
SELECT cust_id FROM art_ord GROUP BY cust_id
HAVING cust_id NOT IN (SELECT id FROM art_d) ORDER BY 1;

-- begin-expected
-- columns: cust_id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT cust_id FROM art_ord GROUP BY cust_id
HAVING sum(total) > ALL (SELECT id FROM art_d) ORDER BY 1;

-- begin-expected
-- columns: cust_id
-- row: 1
-- row: 2
-- end-expected
SELECT cust_id FROM art_ord GROUP BY cust_id
HAVING cust_id = ANY (SELECT id FROM art_d) ORDER BY 1;

-- begin-expected
-- columns: region | count
-- row: APAC, 1
-- row: EU, 2
-- end-expected
SELECT region, count(*) FROM art_cust GROUP BY region
HAVING region IN (SELECT region FROM art_cust WHERE active) ORDER BY 1;

-- an IN over a literal list, a scalar subquery and an EXISTS all still work

-- begin-expected
-- columns: cust_id | sum
-- row: 1, 150.50
-- row: 3, 30.00
-- end-expected
SELECT cust_id, sum(total) FROM art_ord GROUP BY cust_id HAVING cust_id IN (1, 3) ORDER BY 1;

-- begin-expected
-- columns: cust_id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT cust_id FROM art_ord GROUP BY cust_id
HAVING (SELECT max(id) FROM art_d) > 1 ORDER BY 1;

-- begin-expected
-- columns: cust_id
-- row: 1
-- row: 2
-- end-expected
SELECT cust_id FROM art_ord GROUP BY cust_id
HAVING EXISTS (SELECT 1 FROM art_d WHERE id = cust_id) ORDER BY 1;

-- 10: the ordinary shapes around every rule touched here

-- begin-expected
-- columns: region | count
-- row: APAC, 1
-- row: EU, 2
-- row: US, 1
-- end-expected
SELECT region, count(*) FROM art_cust GROUP BY region ORDER BY 1;

-- begin-expected
-- columns: region | c
-- row: EU, 2
-- row: APAC, 1
-- row: US, 1
-- end-expected
SELECT region, count(*) c FROM art_cust GROUP BY region ORDER BY c DESC, region;

-- begin-expected
-- columns: region | count
-- row: EU, 2
-- row: APAC, 1
-- row: US, 1
-- end-expected
SELECT region, count(*) FROM art_cust GROUP BY region ORDER BY 2 DESC, 1;

-- begin-expected
-- columns: region | count
-- row: EU, 2
-- row: APAC, 1
-- row: US, 1
-- end-expected
SELECT region, count(*) FROM art_cust GROUP BY region ORDER BY count(*) DESC, region;

-- begin-expected
-- columns: region | count
-- row: EU, 2
-- end-expected
SELECT region, count(*) FROM art_cust GROUP BY region HAVING count(*) > 1 ORDER BY 1;

-- begin-expected
-- columns: region
-- row: APAC
-- row: EU
-- row: US
-- end-expected
SELECT DISTINCT region FROM art_cust ORDER BY 1;

-- begin-expected
-- columns: region | name
-- row: APAC, Dee
-- row: EU, Ann
-- row: US, Bob
-- end-expected
SELECT DISTINCT ON (region) region, name FROM art_cust ORDER BY region, name;

-- begin-expected
-- columns: region | grouping | count
-- row: APAC, 0, 1
-- row: EU, 0, 2
-- row: US, 0, 1
-- row: NULL, 1, 4
-- end-expected
SELECT region, grouping(region), count(*) FROM art_cust GROUP BY ROLLUP(region) ORDER BY 2, 1;

-- begin-expected
-- columns: cust_id | sum | row_number
-- row: 1, 150.50, 1
-- row: 2, 75.25, 2
-- row: 3, 30.00, 3
-- end-expected
SELECT cust_id, sum(total), row_number() OVER (ORDER BY sum(total) DESC)
FROM art_ord GROUP BY cust_id ORDER BY 1;

-- begin-expected
-- columns: id | total | sum
-- row: 10, 100.00, 100.00
-- row: 11, 50.50, 150.50
-- row: 12, 75.25, 225.75
-- row: 13, 10.00, 235.75
-- row: 14, 20.00, 255.75
-- end-expected
SELECT id, total, sum(total) OVER (ORDER BY id) FROM art_ord ORDER BY id;

-- begin-expected
-- columns: region | sum
-- row: APAC, NULL
-- row: EU, 180.50
-- row: US, 75.25
-- end-expected
SELECT c.region, sum(o.total) FROM art_cust c
LEFT JOIN art_ord o ON o.cust_id = c.id GROUP BY c.region ORDER BY 1;

-- begin-expected
-- columns: id | s
-- row: 1, 150.50
-- row: 2, 75.25
-- row: 3, 30.00
-- row: 4, NULL
-- end-expected
SELECT c.id, x.s FROM art_cust c
JOIN LATERAL (SELECT sum(total) s FROM art_ord o WHERE o.cust_id = c.id) x ON true ORDER BY 1;

-- begin-expected
-- columns: cust_id | s
-- row: 1, 150.50
-- row: 2, 75.25
-- row: 3, 30.00
-- end-expected
SELECT * FROM art_v ORDER BY 1;

-- begin-expected
-- columns: cust_id | s
-- row: 1, 150.50
-- row: 2, 75.25
-- row: 3, 30.00
-- end-expected
WITH g AS (SELECT cust_id, sum(total) s FROM art_ord GROUP BY cust_id) SELECT * FROM g ORDER BY 1;

-- begin-expected
-- columns: cust_id | s
-- row: 1, 150.50
-- row: 2, 75.25
-- end-expected
SELECT * FROM (SELECT cust_id, sum(total) s FROM art_ord GROUP BY cust_id) t
WHERE t.s > 40 ORDER BY 1;

-- begin-expected
-- columns: cust_id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT cust_id FROM art_ord GROUP BY cust_id UNION SELECT id FROM art_d ORDER BY 1;

-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY id) rn FROM art_ord) sub
WHERE sub.rn >= 1 ORDER BY 1;

-- cleanup
DROP VIEW IF EXISTS art_v CASCADE;
DROP TABLE IF EXISTS art_ord CASCADE;
DROP TABLE IF EXISTS art_t2 CASCADE;
DROP TABLE IF EXISTS art_d CASCADE;
DROP TABLE IF EXISTS art_ev CASCADE;
DROP TABLE IF EXISTS art_cust CASCADE;
