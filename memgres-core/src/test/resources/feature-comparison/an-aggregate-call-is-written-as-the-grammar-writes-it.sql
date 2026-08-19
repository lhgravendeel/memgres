-- ============================================================================
-- An aggregate call is written the way the grammar writes it, and resolved by its signature
--
-- The argument list of a call may be introduced by ALL or by DISTINCT -- one of them, and then
-- arguments. ALL is the default written down, so it belongs to every call and not only to the
-- aggregates: abs(ALL -1) is abs(-1). A star is not an argument list, so neither word may introduce
-- one.
--
-- The star in f(*) says which rows to accumulate over, not what to accumulate: the call has no
-- arguments at all, and resolves only against a signature declared over none. count is the only
-- aggregate that has one. The same fact read the other way is why a parameterless aggregate is not
-- written with an empty argument list -- the list is not what is empty.
--
-- GROUPING is a production of the grammar in its own right, spelled like a call but written only as
-- GROUPING ( expr, ... ). It takes at least one argument, takes neither ALL nor DISTINCT, and admits
-- nothing after its closing parenthesis.
--
-- What may follow a call is WITHIN GROUP, then FILTER, then OVER -- in that order, because that is
-- the order the grammar writes them in. So an ordered-set aggregate may carry a FILTER, which
-- chooses the rows it accumulates as it does for any other aggregate.
--
-- A direct argument written as a literal has no type of its own and takes the one the signature
-- declares for it: a percentile fraction is a double precision, and a hypothetical-set aggregate's
-- direct argument is the value being ranked and has its sort column's type.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE ag_a (v int, g text);
INSERT INTO ag_a VALUES (10, 'x'), (20, 'x'), (30, 'y');

-- ============================================================================
-- ALL is the other half of DISTINCT
-- ============================================================================
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(ALL v) FROM ag_a;
-- begin-expected
-- columns: sum
-- row: 60
-- end-expected
SELECT sum(ALL v) FROM ag_a;
-- begin-expected
-- columns: avg
-- row: 20.0000000000000000
-- end-expected
SELECT avg(ALL v) FROM ag_a;
-- begin-expected
-- columns: min | max
-- row: 10 | 30
-- end-expected
SELECT min(ALL v), max(ALL v) FROM ag_a;
-- begin-expected
-- columns: string_agg
-- row: 10,20,30
-- end-expected
SELECT string_agg(ALL v::text, ',' ORDER BY v) FROM ag_a;
-- begin-expected
-- columns: array_agg
-- row: {10,20,30}
-- end-expected
SELECT array_agg(ALL v ORDER BY v) FROM ag_a;
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(ALL v) FILTER (WHERE v > 10) FROM ag_a;

-- ALL is the default written down, so it belongs to every call and not only to an aggregate.
-- begin-expected
-- columns: abs
-- row: 1
-- end-expected
SELECT abs(ALL -1);
-- begin-expected
-- columns: upper
-- row: A
-- end-expected
SELECT upper(ALL 'a');

-- One of the two words, and then arguments: the other is not the start of one.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(ALL DISTINCT v) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(DISTINCT ALL v) FROM ag_a;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(DISTINCT v) FROM ag_a;
-- begin-expected
-- columns: array_agg
-- row: {10,20,30}
-- end-expected
SELECT array_agg(DISTINCT v ORDER BY v) FROM ag_a;

-- And DISTINCT still means accumulate a chosen subset, which only an aggregate does.
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT abs(DISTINCT -1);

-- ============================================================================
-- The star says which rows, not what to accumulate
-- ============================================================================
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ag_a;
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FILTER (WHERE v > 10) FROM ag_a;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(v) FROM ag_a;

-- A star is not an argument list, so neither ALL nor DISTINCT may introduce one.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(DISTINCT *) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(ALL *) FROM ag_a;

-- The call has no arguments, so it resolves only against a signature declared over none.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT sum(*) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT max(*) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT min(*) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT avg(*) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT string_agg(*) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT bool_and(*) FROM ag_a;

-- And read the other way: a parameterless aggregate is not written with an empty list.
-- begin-expected-error
-- sqlstate: 42809
-- end-expected-error
SELECT count() FROM ag_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT sum() FROM ag_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT abs() FROM ag_a;

-- ============================================================================
-- GROUPING is a production of the grammar, not a call
-- ============================================================================
-- begin-expected
-- columns: grouping
-- row: 0
-- row: 0
-- row: 0
-- end-expected
SELECT grouping(v) FROM ag_a GROUP BY v ORDER BY 1;
-- begin-expected
-- columns: grouping
-- row: 0
-- row: 0
-- row: 0
-- row: 1
-- row: 1
-- row: 1
-- row: 3
-- end-expected
SELECT grouping(v, g) FROM ag_a GROUP BY ROLLUP(v, g) ORDER BY 1;
-- begin-expected
-- columns: grouping
-- row: 1
-- row: 1
-- row: 1
-- row: 2
-- row: 2
-- end-expected
SELECT grouping(v, g) FROM ag_a GROUP BY GROUPING SETS ((v), (g)) ORDER BY 1;

-- It takes at least one argument, and neither ALL nor DISTINCT.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT grouping() FROM ag_a GROUP BY v;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT grouping(DISTINCT v) FROM ag_a GROUP BY v;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT grouping(ALL v) FROM ag_a GROUP BY v;

-- And nothing may follow its closing parenthesis.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT grouping(v) OVER () FROM ag_a GROUP BY v;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT grouping(v) FILTER (WHERE true) FROM ag_a GROUP BY v;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT grouping(v) WITHIN GROUP (ORDER BY v) FROM ag_a GROUP BY v;

-- ============================================================================
-- WITHIN GROUP, then FILTER, then OVER
-- ============================================================================
-- begin-expected
-- columns: percentile_cont
-- row: 25
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a;
-- begin-expected
-- columns: percentile_disc
-- row: 20
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a;
-- begin-expected
-- columns: mode
-- row: 20
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a;
-- begin-expected
-- columns: rank
-- row: 1
-- end-expected
SELECT rank(20) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a;
-- begin-expected
-- columns: dense_rank
-- row: 2
-- end-expected
SELECT dense_rank(20) WITHIN GROUP (ORDER BY v) FILTER (WHERE v < 30) FROM ag_a;
-- begin-expected
-- columns: cume_dist
-- row: 0.6666666666666666
-- end-expected
SELECT cume_dist(20) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a;

-- A predicate nothing satisfies leaves the aggregate with no rows, as it does for any other.
-- begin-expected
-- columns: mode
-- row: NULL
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY v) FILTER (WHERE false) FROM ag_a;
-- begin-expected
-- columns: percentile_cont
-- row: NULL
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE false) FROM ag_a;
-- begin-expected
-- columns: sum
-- row: NULL
-- end-expected
SELECT sum(v) FILTER (WHERE false) FROM ag_a;

-- The same, once a group is what is being accumulated.
-- begin-expected
-- columns: g | percentile_disc
-- row: x | 20
-- row: y | 30
-- end-expected
SELECT g, percentile_disc(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a GROUP BY g ORDER BY g;
-- begin-expected
-- columns: g | count
-- row: x | 1
-- row: y | 1
-- end-expected
SELECT g, count(*) FILTER (WHERE v > 10) FROM ag_a GROUP BY g ORDER BY g;

-- Written the other way round there is nowhere for the WITHIN GROUP to go.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT percentile_cont(0.5) FILTER (WHERE true) WITHIN GROUP (ORDER BY v) FROM ag_a;
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(v) FILTER (WHERE true) WITHIN GROUP (ORDER BY v) FROM ag_a;

-- And a FILTER on a window call is read where it always was.
-- begin-expected
-- columns: count
-- row: 2
-- row: 2
-- row: 2
-- end-expected
SELECT count(*) FILTER (WHERE v > 10) OVER () FROM ag_a ORDER BY 1;
-- begin-expected
-- columns: sum
-- row: 20
-- row: 50
-- row: NULL
-- end-expected
SELECT sum(v) FILTER (WHERE v > 10) OVER (ORDER BY v) FROM ag_a ORDER BY 1;

-- ============================================================================
-- A direct argument has the type its signature declares
-- ============================================================================
-- begin-expected
-- columns: percentile_cont
-- row: 20
-- end-expected
SELECT percentile_cont('0.5') WITHIN GROUP (ORDER BY v) FROM ag_a;
-- begin-expected
-- columns: percentile_disc
-- row: 20
-- end-expected
SELECT percentile_disc('0.5') WITHIN GROUP (ORDER BY v) FROM ag_a;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT percentile_cont('zz') WITHIN GROUP (ORDER BY v) FROM ag_a;

-- A hypothetical-set aggregate's direct argument is the value being ranked, so it has the type
-- of the column it is ranked against.
-- begin-expected
-- columns: rank
-- row: 2
-- end-expected
SELECT rank('20') WITHIN GROUP (ORDER BY v) FROM ag_a;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT rank('zz') WITHIN GROUP (ORDER BY v) FROM ag_a;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT dense_rank('zz') WITHIN GROUP (ORDER BY v) FROM ag_a;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT percent_rank('zz') WITHIN GROUP (ORDER BY v) FROM ag_a;
-- begin-expected
-- columns: rank
-- row: 4
-- end-expected
SELECT rank('zz') WITHIN GROUP (ORDER BY g) FROM ag_a;
-- begin-expected
-- columns: rank
-- row: 2
-- end-expected
SELECT rank(20, 'x') WITHIN GROUP (ORDER BY v, g) FROM ag_a;
-- begin-expected
-- columns: mode
-- row: x
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY g) FROM ag_a;

-- teardown
DROP TABLE ag_a;
