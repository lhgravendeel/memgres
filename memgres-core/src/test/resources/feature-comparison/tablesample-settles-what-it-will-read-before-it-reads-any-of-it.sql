-- ============================================================================
-- TABLESAMPLE settles what it will read before it reads any of it
--
-- A sampled scan decides four things from what was written, in that order and before a single page
-- is touched: which relation, which method, how many arguments the method was given, and what types
-- those arguments are. Only then does it read their values.
--
-- The method is named by an identifier like any other, so a quoted one keeps the case it was written
-- in and an unquoted one is folded. How many arguments a method takes is the method's own business
-- rather than the grammar's, which is why a second one is not a syntax error -- bernoulli is asked,
-- and says it wanted one. Both arguments are declared over a type: the percentage is a real and the
-- seed a double precision, and a value PostgreSQL would not put there without being told to is
-- refused by type rather than converted. A value with no type of its own is read by the target's own
-- input function instead, so '100' is a percentage while '100'::text is not.
--
-- How much of a relation to read is settled before any of it is read, so the arguments sit outside
-- the relation the clause is attached to even though they are written next to its name.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE ts_a (id int, v text);
INSERT INTO ts_a VALUES (1, 'one'), (2, 'two'), (3, 'three');
CREATE VIEW ts_v AS SELECT id FROM ts_a;
CREATE MATERIALIZED VIEW ts_m AS SELECT id FROM ts_a;
CREATE SEQUENCE ts_s;

-- ============================================================================
-- A relation whose pages are its own, or none at all
-- ============================================================================
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100);
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (0);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_m TABLESAMPLE BERNOULLI (100);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT count(*) FROM ts_v TABLESAMPLE BERNOULLI (100);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT count(*) FROM ts_s TABLESAMPLE BERNOULLI (100);
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM ts_nope TABLESAMPLE BERNOULLI (100);

-- The relation is settled first, so what follows it is never reached.
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT count(*) FROM ts_v TABLESAMPLE nosuch (100);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT count(*) FROM ts_v TABLESAMPLE BERNOULLI (50, 50);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT count(*) FROM ts_v TABLESAMPLE BERNOULLI (true);
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM ts_nope TABLESAMPLE nosuch (100);
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM ts_nope TABLESAMPLE BERNOULLI (50, 50);
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM ts_nope TABLESAMPLE BERNOULLI (true);

-- ============================================================================
-- The method is an identifier, quoted or not
-- ============================================================================
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE bernoulli (100);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE SySTeM (100);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE "bernoulli" (100);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE "system" (100);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE "SYSTEM" (100);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE "Bernoulli" (100);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE "no such" (100);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE nosuch (100);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE NOSUCH (100);

-- And it is looked up before anything is asked of its arguments.
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE nosuch (100, 100);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE nosuch (true);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE nosuch (id);

-- ============================================================================
-- How many arguments the method wanted is the method's own business
-- ============================================================================
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (50, 50);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100, 100);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100, 100, 100);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (50, true);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (id, id);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (50, 50) REPEATABLE (true);
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ();

-- ============================================================================
-- The percentage is a real, and the seed a double precision
-- ============================================================================
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100::smallint);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100::bigint);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100::numeric);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100::float4);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100::float8);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('100');
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (50 + 50);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ((SELECT 100));

-- Anything PostgreSQL would not put there without being told to is refused by type.
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (true);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('100'::text);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (NULL::text);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('x'::char);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('1 day'::interval);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('{1}'::int[]);

-- The seed answers the same question about double precision.
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE (5);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE (0.0);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE (1e300);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE (-5);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE (100::numeric);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE (true);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE ('100'::text);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE ('1 day'::interval);
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE ('abc');

-- Both types are settled before either value is read.
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (NULL) REPEATABLE (true);
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (true) REPEATABLE (true);

-- ============================================================================
-- A value with no type of its own is read by the type that was wanted
-- ============================================================================
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('abc');
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM (100) REPEATABLE ('xyz');
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('0');
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('1e400');

-- ============================================================================
-- Then the values, and only then
-- ============================================================================
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (NULL);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (NULL::float8);
-- begin-expected-error
-- sqlstate: 2202G
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100) REPEATABLE (NULL);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (NULL) REPEATABLE (NULL);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (-1);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (101);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('nan'::float8);
-- begin-expected-error
-- sqlstate: 2202H
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI ('infinity'::float8);
-- begin-expected-error
-- sqlstate: 2202G
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (-1) REPEATABLE (NULL);

-- ============================================================================
-- The arguments sit outside the relation the clause is attached to
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (id);
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (v);
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM ts_a t TABLESAMPLE BERNOULLI (id);
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (ts_a.id);
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (100) REPEATABLE (id);
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE BERNOULLI (nocol);

-- ============================================================================
-- The clause belongs to a relation, not to whatever produced rows
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(*) FROM (SELECT id FROM ts_a) s TABLESAMPLE BERNOULLI (100);
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(*) FROM generate_series(1, 10) g TABLESAMPLE BERNOULLI (100);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a AS t TABLESAMPLE BERNOULLI (100);
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM ts_a t TABLESAMPLE BERNOULLI (100) REPEATABLE (1);
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT count(*) FROM ts_a TABLESAMPLE SYSTEM;

-- teardown
DROP SEQUENCE ts_s;
DROP MATERIALIZED VIEW ts_m;
DROP VIEW ts_v;
DROP TABLE ts_a;
