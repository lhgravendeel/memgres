-- ============================================================================
-- An aggregate is a function the catalogue holds.
--
-- It is declared however the grammar allows it to be, found by its name and the
-- types it was written with, and refused wherever no function of that shape may
-- stand. A call that names one of the ordered-set aggregates without WITHIN GROUP
-- is not a syntax error: it is a signature that either answers or does not. And a
-- transform expression is not a query, so nothing that needs one may appear in it.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS zz_agg CASCADE;
CREATE TABLE zz_agg (i int, n numeric, f float8, s smallint, g bigint,
                     t text, d date, ts timestamp, iv interval, b bool,
                     m money, u uuid);
INSERT INTO zz_agg VALUES (1, 1.5, 1.5, 1, 1, 'x', '2020-01-01', '2020-01-01 00:00',
                     '1 hour', true, 1.00, '00000000-0000-0000-0000-000000000001');

-- ============================================================================
-- An ordered-set aggregate is catalogued under its whole signature
-- ============================================================================
-- The direct arguments and the ordered ones are one signature, so a call written
-- without WITHIN GROUP is resolved against it like any other call: the right number
-- of arguments is a call missing its clause, and any other number is no function.
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT percentile_cont(0.5, 1) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT percentile_disc(0.5, 1) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT mode(1) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT mode(i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_disc(0.5) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT mode() FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(ARRAY[0.5]) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT mode(1, 2) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_disc(0.5, 1, 2) FROM zz_agg;
-- An OVER clause is read after the call has been resolved, so this is still the
-- missing WITHIN GROUP and not an OVER a plain function has no use for.
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT mode(1) OVER () FROM zz_agg;
-- rank and its kin are window functions as well and take any number of arguments,
-- so what tells the two answers apart is whether any were written at all.
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT rank() FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT rank(1) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT dense_rank(1) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT percent_rank(1) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT cume_dist(1) FROM zz_agg;

-- ============================================================================
-- The types a percentile is written with decide whether it exists
-- ============================================================================
-- percentile_cont interpolates, so it is declared over what can be interpolated:
-- the numeric types and interval. percentile_disc picks a value out and takes any
-- type at all. Both take their fraction as a double precision.
-- begin-expected
-- columns: percentile_cont
-- row: 1.5
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) FROM zz_agg;
-- begin-expected
-- columns: percentile_cont
-- row: 1.5
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY f) FROM zz_agg;
-- begin-expected
-- columns: percentile_cont
-- row: 1
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY s) FROM zz_agg;
-- begin-expected
-- columns: percentile_cont
-- row: 1
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY g) FROM zz_agg;
-- begin-expected
-- columns: percentile_cont
-- row: 01:00:00
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY iv) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY d) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ts) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY m) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY u) FROM zz_agg;
-- begin-expected
-- columns: percentile_disc
-- row: x
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY t) FROM zz_agg;
-- begin-expected
-- columns: percentile_disc
-- row: 2020-01-01
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY d) FROM zz_agg;
-- begin-expected
-- columns: mode
-- row: x
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY t) FROM zz_agg;
-- A fraction is a double precision, and a column of another type is not one.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(t) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_disc(t) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(d) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(ARRAY['a']) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- A quoted literal carries no type of its own and takes the declared one.
-- begin-expected
-- columns: percentile_cont
-- row: 1
-- end-expected
SELECT percentile_cont('0.5') WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected
-- columns: percentile_cont
-- row: 5
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY '5') FROM zz_agg;
-- Only one column may be ordered by: two of them is a signature nothing answers to.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i, g) FROM zz_agg;
-- The call is settled before a page is read, so an empty group answers the same.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY d) FROM zz_agg WHERE false;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT percentile_cont(t) WITHIN GROUP (ORDER BY i) FROM zz_agg WHERE false;
-- begin-expected
-- columns: percentile_cont
-- row: NULL
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY iv) FROM zz_agg WHERE false;

-- ============================================================================
-- A fraction out of range is named, not described
-- ============================================================================
-- The message carries the value it was handed, written to six significant digits.
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(2.0) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(-1) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(1e10) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(1234567) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(1.234567891) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(-0.0001) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc('nan'::float8) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc('infinity'::float8) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_cont(ARRAY[0.5, 2.5]) WITHIN GROUP (ORDER BY i) FROM zz_agg;
-- The fraction is a direct argument, taken once for the whole group rather than per
-- row, so a group with nothing in it is handed one just the same and refuses it.
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY i) FROM zz_agg WHERE false;

-- ============================================================================
-- percentile_cont interpolates an interval as an interval
-- ============================================================================
-- An interval keeps its months, days and microseconds apart because a month is not
-- a fixed number of days, and the fraction of each is carried down to the next.
DROP TABLE IF EXISTS zz_agi CASCADE;
CREATE TABLE zz_agi (iv interval);
INSERT INTO zz_agi VALUES ('1 hour'), ('3 hours');
-- begin-expected
-- columns: half
-- row: 02:00:00
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY iv) AS half FROM zz_agi;
-- begin-expected
-- columns: threequarters
-- row: 02:30:00
-- end-expected
SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY iv) AS threequarters FROM zz_agi;
-- begin-expected
-- columns: each
-- row: {01:00:00,02:00:00,03:00:00}
-- end-expected
SELECT percentile_cont(ARRAY[0.0, 0.5, 1.0]) WITHIN GROUP (ORDER BY iv) AS each FROM zz_agi;
-- begin-expected
-- columns: picked
-- row: 01:00:00
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY iv) AS picked FROM zz_agi;
DELETE FROM zz_agi;
INSERT INTO zz_agi VALUES ('1 mon'), ('4 mons 10 days');
-- begin-expected
-- columns: halfmon
-- row: 2 mons 20 days
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY iv) AS halfmon FROM zz_agi;
-- begin-expected
-- columns: partmon
-- row: 1 mon 30 days
-- end-expected
SELECT percentile_cont(0.3) WITHIN GROUP (ORDER BY iv) AS partmon FROM zz_agi;
-- The same cascade a fraction of an interval goes through anywhere.
-- begin-expected
-- columns: a
-- row: 30 days
-- end-expected
SELECT interval '3 mons 10 days' * 0.3 AS a;
-- begin-expected
-- columns: b
-- row: -30 days
-- end-expected
SELECT interval '3 mons 10 days' * (-0.3) AS b;
-- begin-expected
-- columns: c
-- row: 31 days 14:08:51.5
-- end-expected
SELECT interval '1 mon 15 days 3:04:05' * 0.7 AS c;
-- begin-expected
-- columns: d
-- row: 33 years 3 mons 18 days
-- end-expected
SELECT interval '100 years' * 0.333 AS d;
DROP TABLE IF EXISTS zz_agi CASCADE;

-- ============================================================================
-- A transform expression is not a query
-- ============================================================================
-- USING converts one row's old value into the new one, on that row alone: there is
-- no query around it for a sub-query to run in, no group for an aggregate to be
-- taken over and no window for a window call to be numbered against.
DROP TABLE IF EXISTS zz_agt CASCADE;
CREATE TABLE zz_agt (a int);
INSERT INTO zz_agt VALUES (1), (2);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING (SELECT 1);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING array(SELECT 1)::text;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING a IN (SELECT 1);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING a = ANY (SELECT 1);
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING count(*);
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING sum(a);
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING count(*)::text;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING grouping(a)::text;
-- begin-expected-error
-- sqlstate: 42P20
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING row_number() OVER ();
-- begin-expected-error
-- sqlstate: 42P20
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING count(*) OVER ();
-- What the expression names is resolved before the clause is judged, and the target
-- type and the column being retyped are both looked at after it.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING nosuch(a);
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING count(nocol);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING nosuch(a) + count(*);
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN a TYPE nosuchtype USING count(*);
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
ALTER TABLE zz_agt ALTER COLUMN nocol TYPE text USING count(*);
-- A transform expression that is one converts the column as it always did, and the
-- clauses that are queries in their own right are untouched.
ALTER TABLE zz_agt ALTER COLUMN a TYPE text USING a::text;
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM zz_agt ORDER BY a;
DROP TABLE IF EXISTS zz_agt CASCADE;

-- ============================================================================
-- An aggregate is declared however the grammar allows
-- ============================================================================
-- The spelling that predates argument lists names the argument type inside the
-- parameter list, in whatever order it likes, and BASETYPE = ANY takes none.
DROP AGGREGATE IF EXISTS zz_aggsum(int) CASCADE;
DROP AGGREGATE IF EXISTS zz_aggmax(int) CASCADE;
DROP FUNCTION IF EXISTS zz_aggadd(int, int) CASCADE;
CREATE FUNCTION zz_aggadd(int, int) RETURNS int AS 'SELECT $1 + $2' LANGUAGE sql IMMUTABLE;
CREATE AGGREGATE zz_aggsum (BASETYPE = int, SFUNC = zz_aggadd, STYPE = int, INITCOND = '0');
CREATE AGGREGATE zz_aggmax (SFUNC = zz_aggadd, BASETYPE = int, STYPE = int, INITCOND = '10');
-- begin-expected
-- columns: zz_aggsum
-- row: 1
-- end-expected
SELECT zz_aggsum(i) FROM zz_agg;
-- begin-expected
-- columns: zz_aggmax
-- row: 11
-- end-expected
SELECT zz_aggmax(i) FROM zz_agg;
-- One so declared is a pg_proc row like any other, so a name naming it names it.
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'zz_aggsum';
-- begin-expected
-- columns: prokind
-- row: a
-- end-expected
SELECT prokind FROM pg_proc WHERE proname = 'zz_aggsum';
-- begin-expected
-- columns: pronargs
-- row: 1
-- end-expected
SELECT pronargs FROM pg_proc WHERE proname = 'zz_aggsum';
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_aggregate a JOIN pg_proc p ON p.oid = a.aggfnoid WHERE p.proname = 'zz_aggsum';
-- begin-expected
-- columns: text
-- row: zz_aggsum(integer)
-- end-expected
SELECT 'zz_aggsum(int)'::regprocedure::text;
-- begin-expected
-- columns: text
-- row: zz_aggsum
-- end-expected
SELECT 'zz_aggsum'::regproc::text;
-- begin-expected
-- columns: to_regprocedure
-- row: zz_aggsum(integer)
-- end-expected
SELECT to_regprocedure('zz_aggsum(int)')::text;
-- begin-expected
-- columns: to_regproc
-- row: zz_aggsum
-- end-expected
SELECT to_regproc('zz_aggsum')::text;
-- begin-expected
-- columns: to_regprocedure
-- row: NULL
-- end-expected
SELECT to_regprocedure('zz_aggsum(text)')::text;
DROP AGGREGATE IF EXISTS zz_aggsum(int) CASCADE;
DROP AGGREGATE IF EXISTS zz_aggmax(int) CASCADE;
DROP FUNCTION IF EXISTS zz_aggadd(int, int) CASCADE;

-- teardown
DROP TABLE IF EXISTS zz_agg CASCADE;
