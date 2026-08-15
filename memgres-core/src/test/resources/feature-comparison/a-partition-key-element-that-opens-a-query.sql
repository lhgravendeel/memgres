-- ============================================================================
-- A partition key element that opens a query, and which fault a key is refused
-- for
--
-- A partition key element is a column name, a call, or an expression in
-- parentheses of its own. The pair of parentheses a sub-query is written with is
-- the pair the element has already spent, so a query written with one pair stops
-- at the word that opens it, and only the doubled form reaches the analysis that
-- refuses a sub-query in a key for what it is.
--
-- PostgreSQL analyses a key expression from the leaves outwards, exactly as it
-- analyses one written in a query, and reports whichever fault it reaches first:
-- a name the relation does not carry, an aggregate, a window call and a
-- sub-query all come out of the one pass, and a call's arguments are settled
-- before the call itself is placed.
--
-- And every other expression a query may hold stands in a key: an operator, a
-- call, a cast, a CASE, a subscript, a BETWEEN and an IN list are all keys
-- PostgreSQL takes.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- A key element written with one pair of parentheses stops at the word that
-- opens it
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SELECT"
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((SELECT 1));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SELECT"
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE (SELECT 1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SELECT"
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE (i, (SELECT 1));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SELECT"
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((SELECT 1), i);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "TABLE"
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((TABLE zzm3sd_pk));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "WITH"
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((WITH q AS (SELECT 1) SELECT 1 FROM q));

-- two pairs, and the element is an expression the analysis reaches and refuses
-- for what it is
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE (((SELECT 1)));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE (((SELECT 1) + 1));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY LIST (((SELECT 1)));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY HASH (((SELECT 1)));

-- ============================================================================
-- Which fault in a key expression is reported
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((sum(i) + nosuch));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((nosuch + sum(i)));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((sum(i) + nosuch), nosuch2);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((row_number() OVER () + nosuch));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((row_number() OVER (ORDER BY nosuch)));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE (((SELECT 1) + nosuch));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((nosuch + (SELECT 1)));

-- a call's arguments are analysed before the call is placed, so the name inside
-- an aggregate is what PostgreSQL reports and not the aggregate
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((sum(nosuch)));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((abs(i) FILTER (WHERE nosuch)));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((CASE WHEN nosuch THEN sum(i) END));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((CASE WHEN true THEN sum(i) ELSE nosuch END));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((nosuch + nosuch2));

-- a system column resolves like any other, so the name that is not there is the
-- fault wherever it stands beside one
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((xmin + nosuch));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((nosuch + xmin));

-- and a name is reached before the key is judged for changing its answer
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_pk (i int, k int) PARTITION BY RANGE ((random()::int + nosuch));

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname = 'zzm3sd_pk';

-- ============================================================================
-- The keys PostgreSQL does take
-- ============================================================================

CREATE TABLE zzm3sd_k1 (i int, k int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzm3sd_k2 (i int, k int, s text) PARTITION BY RANGE ((i));
CREATE TABLE zzm3sd_k3 (i int, k int, s text) PARTITION BY RANGE ((i + k));
CREATE TABLE zzm3sd_k4 (i int, k int, s text) PARTITION BY LIST (lower(s));
CREATE TABLE zzm3sd_k5 (i int, k int, s text) PARTITION BY RANGE (abs(i));
CREATE TABLE zzm3sd_k6 (i int, k int, s text) PARTITION BY RANGE (i, k);
CREATE TABLE zzm3sd_k7 (i int, k int, s text) PARTITION BY RANGE ((CASE WHEN i > 0 THEN 1 ELSE 2 END));
CREATE TABLE zzm3sd_k8 (i int, k int, s text) PARTITION BY LIST ((s || 'x'));
CREATE TABLE zzm3sd_k9 (i int, k int, s text) PARTITION BY RANGE ((i::bigint));
CREATE TABLE zzm3sd_ka (i int, a int[], s text) PARTITION BY RANGE ((a[1]));
CREATE TABLE zzm3sd_kb (i int, k int, s text) PARTITION BY HASH ((i + 1));
CREATE TABLE zzm3sd_kc (i int, k int, s text) PARTITION BY RANGE ((i BETWEEN 1 AND 9));
CREATE TABLE zzm3sd_kd (i int, k int, s text) PARTITION BY LIST ((i IN (1, 2)));
CREATE TABLE zzm3sd_ke (i int, k int, s text) PARTITION BY RANGE ((greatest(i, k)));

-- begin-expected
-- columns: n
-- row: 14
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname LIKE 'zzm3sd\_k%' AND relkind = 'p';

-- begin-expected
-- columns: relname | keydef
-- row: zzm3sd_k1 | RANGE (i)
-- row: zzm3sd_k4 | LIST (lower(s))
-- row: zzm3sd_k5 | RANGE (abs(i))
-- row: zzm3sd_k6 | RANGE (i, k)
-- end-expected
SELECT relname, pg_get_partkeydef(oid) AS keydef FROM pg_class WHERE relname IN ('zzm3sd_k1','zzm3sd_k4','zzm3sd_k5','zzm3sd_k6') ORDER BY relname;

DROP TABLE zzm3sd_k1;
DROP TABLE zzm3sd_k2;
DROP TABLE zzm3sd_k3;
DROP TABLE zzm3sd_k4;
DROP TABLE zzm3sd_k5;
DROP TABLE zzm3sd_k6;
DROP TABLE zzm3sd_k7;
DROP TABLE zzm3sd_k8;
DROP TABLE zzm3sd_k9;
DROP TABLE zzm3sd_ka;
DROP TABLE zzm3sd_kb;
DROP TABLE zzm3sd_kc;
DROP TABLE zzm3sd_kd;
DROP TABLE zzm3sd_ke;
