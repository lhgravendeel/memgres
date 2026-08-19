-- ============================================================================
-- A branch and an operand are settled by their types, not by the values they turn out to hold.
--
-- PostgreSQL resolves an expression before it runs one: a prefix operator is chosen from the type
-- its operand is written with, a simple CASE builds one equality per WHEN out of the operand's type
-- and the value's, a CASE settles one result type across its branches, and an IN list and an ANY or
-- ALL settle one type across both sides. Only then is a value read. memgres was deciding all of
-- these from the values as they came out, which answered questions PostgreSQL refuses to plan and
-- named the wrong fault when it did complain.
--
-- Only a type the query itself writes down takes part. A column's type here is whatever the engine
-- settled on, and refusing an operator on the strength of that would reject SQL PostgreSQL runs.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS zz_ty CASCADE;
CREATE TABLE zz_ty (id int, v int, t text, d date);
INSERT INTO zz_ty VALUES (1, 10, 'a', DATE '2020-01-01'), (2, 20, 'b', DATE '2020-06-01');

-- ============================================================================
-- A prefix operator is resolved from the type its operand is written with
-- ============================================================================
-- Every arithmetic prefix operator is declared over the numbers, and over nothing else.
-- begin-expected
-- columns: ?column?
-- row: -1
-- end-expected
SELECT -(1::smallint);
-- begin-expected
-- columns: ?column?
-- row: -1
-- end-expected
SELECT -(1::int);
-- begin-expected
-- columns: ?column?
-- row: -1
-- end-expected
SELECT -(1::bigint);
-- begin-expected
-- columns: ?column?
-- row: -1
-- end-expected
SELECT -(1::numeric);
-- begin-expected
-- columns: ?column?
-- row: -1
-- end-expected
SELECT -(1::real);
-- begin-expected
-- columns: ?column?
-- row: -1.5
-- end-expected
SELECT -('1.5'::float8);
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT +(1::smallint);
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT +(1::bigint);
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT +(1::numeric);
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT @('-1'::int);
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT @('-1'::numeric);
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT @(1::smallint);
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT @(1::real);
-- begin-expected
-- columns: ?column?
-- row: 2
-- end-expected
SELECT |/(4::int);
-- begin-expected
-- columns: ?column?
-- row: 2
-- end-expected
SELECT ||/(8::int);
-- Minus is also declared over a span of time; the others are not.
-- begin-expected
-- columns: ?column?
-- row: -1 days
-- end-expected
SELECT -(interval '1 day');
-- begin-expected
-- columns: ?column?
-- row: -01:00:00
-- end-expected
SELECT -('01:00:00'::interval);
-- A null of a type the operator has is still an operand of that type.
-- begin-expected
-- columns: ?column?
-- row: NULL
-- end-expected
SELECT -(NULL::int);
-- begin-expected
-- columns: ?column?
-- row: NULL
-- end-expected
SELECT -(NULL::numeric);
-- Everything else has no operator to be written in front of, and says so in the singular.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('abc'::text);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -(NULL::text);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('x'::varchar);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('x'::char(1));
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('x'::name);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -(true);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -(date '2020-01-01');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('2020-01-01'::timestamp);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('2020-01-01'::timestamptz);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('11111111-1111-1111-1111-111111111111'::uuid);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('{1,2}'::int[]);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('{"a":1}'::json);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('1'::jsonb);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('192.168.0.1'::inet);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -(point '(1,1)');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -('$1'::money);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT -(B'101');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT +('abc'::text);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT +(true);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT +(date '2020-01-01');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT +(interval '1 day');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT +('$1'::money);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT @('abc'::text);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT @(true);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT @('1 day'::interval);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT @('$1'::money);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT @('01:00'::time);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT |/('4'::text);
-- The type a column was given is not a type the query wrote down, so it settles nothing here.
-- begin-expected
-- columns: ?column?
-- row: -20
-- row: -10
-- end-expected
SELECT -v FROM zz_ty ORDER BY 1;
-- begin-expected
-- columns: ?column?
-- row: 10
-- row: 20
-- end-expected
SELECT @(-v) FROM zz_ty ORDER BY 1;
-- begin-expected
-- columns: ?column?
-- row: -1
-- row: -1
-- end-expected
SELECT -length(t) FROM zz_ty ORDER BY 1;

-- ============================================================================
-- An operand that says nothing about its type leaves the operator to be chosen from the candidates
-- ============================================================================
-- Minus negates a number and a span of time both, and PostgreSQL will not choose between them.
-- begin-expected-error
-- sqlstate: 42725
-- end-expected-error
SELECT -'1';
-- begin-expected-error
-- sqlstate: 42725
-- end-expected-error
SELECT -'abc';
-- begin-expected-error
-- sqlstate: 42725
-- end-expected-error
SELECT -(NULL);
-- The rest have only numbers to choose from, so there is nothing to be undecided about.
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT +'1';
-- begin-expected
-- columns: ?column?
-- row: NULL
-- end-expected
SELECT @(NULL);

-- ============================================================================
-- A simple CASE compares its operand with each value, and settles both before it compares
-- ============================================================================
-- The operand and the value are of one family, so there is an equality to test.
-- begin-expected
-- columns: case
-- row: y
-- end-expected
SELECT CASE 1 WHEN 1 THEN 'y' ELSE 'n' END;
-- begin-expected
-- columns: case
-- row: n
-- end-expected
SELECT CASE 1 WHEN 1.5 THEN 'y' ELSE 'n' END;
-- begin-expected
-- columns: case
-- row: y
-- end-expected
SELECT CASE 1 WHEN '1' THEN 'y' ELSE 'n' END;
-- begin-expected
-- columns: case
-- row: y
-- end-expected
SELECT CASE 'a' WHEN 'a' THEN 'y' ELSE 'n' END;
-- begin-expected
-- columns: case
-- row: n
-- end-expected
SELECT CASE 1 WHEN NULL THEN 'y' ELSE 'n' END;
-- begin-expected
-- columns: case
-- row: n
-- end-expected
SELECT CASE 'a' WHEN NULL THEN 'y' ELSE 'n' END;
-- A value written out and not readable as the operand's type is an input error.
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT CASE 1 WHEN 'a' THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT CASE 1::bigint WHEN 'a' THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT CASE true WHEN 'x' THEN 1 ELSE 2 END;
-- An operand that says nothing about its type is text, not whatever the first WHEN turns out to be.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT CASE 'a' WHEN 1 THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT CASE 'a' WHEN 1.5 THEN 1 ELSE 2 END;
-- A value from another family has no equality with the operand at all.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT CASE 1 WHEN true THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT CASE 'a'::text WHEN 1 THEN 1 ELSE 2 END;
-- A column's type settles nothing, so a CASE over one is compared as it always was.
-- begin-expected
-- columns: case
-- row: 1
-- row: 2
-- end-expected
SELECT CASE t WHEN 'a' THEN 1 ELSE 2 END FROM zz_ty ORDER BY 1;
-- begin-expected
-- columns: case
-- row: 1
-- row: 2
-- end-expected
SELECT CASE v WHEN 10 THEN 1 ELSE 2 END FROM zz_ty ORDER BY 1;

-- ============================================================================
-- A CASE settles one result type across its branches before it takes one
-- ============================================================================
-- A branch written out and not readable as the settled type is an input error.
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT CASE WHEN true THEN 1 ELSE 'a' END;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT CASE WHEN 1=1 THEN 'a' ELSE (SELECT 1/0) END;
-- A subquery hands its answer out as a value of some type, so an unknown in it is text by then.
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT CASE WHEN true THEN 'a' ELSE (SELECT 1) END;
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT CASE WHEN true THEN 1 ELSE (SELECT 'a') END;
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT CASE WHEN true THEN 1 ELSE (SELECT NULL) END;
-- A branch written with a type from another family cannot be read as the settled one at all.
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT CASE WHEN true THEN 1 ELSE 'a'::text END;
-- A bare NULL is not a branch that says anything, and does not settle or break anything.
-- begin-expected
-- columns: case
-- row: 1
-- end-expected
SELECT CASE WHEN true THEN 1 ELSE NULL END;

-- ============================================================================
-- Two arrays are matched by what they hold
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT CASE WHEN false THEN ARRAY[1] ELSE ARRAY['a'] END;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT CASE WHEN false THEN ARRAY['a'] ELSE ARRAY[1] END;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT CASE WHEN false THEN ARRAY[1] ELSE ARRAY[NULL] END;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT COALESCE(ARRAY[1], ARRAY['a']);
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT GREATEST(ARRAY[1], ARRAY['a']);
-- An array and a value of its element type are not two arrays, and do not match at all.
-- begin-expected-error
-- sqlstate: 42804
-- end-expected-error
SELECT CASE WHEN false THEN ARRAY[1] ELSE 1 END;
-- A row is not an array: its fields keep their own types rather than settling on one.
-- begin-expected
-- columns: r
-- row: (2,b)
-- end-expected
SELECT CASE WHEN false THEN ROW(1, 'a') ELSE ROW(2, 'b') END AS r;

-- ============================================================================
-- Every entry of an IN list is read as the type the comparison settles on
-- ============================================================================
-- An entry that will not read that way is at fault whether or not an earlier entry matched.
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 1 IN ('x');
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 1.5 IN ('x');
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 1::bigint IN ('x');
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 1 IN (1, 'x');
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 1 IN ('x', 1);
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 1 NOT IN (1, 'x');
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT NULL IN (1, 'x');
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT NULL::int IN (1, 'x');
-- An untyped left side is read as the list's type rather than kept as text.
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 'x' IN (1);
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 'x' IN (1, 2);
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '1' IN (1);
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT '1' IN (2);
-- A list every entry of which reads as the settled type answers as it always did.
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 1 IN (1, 2);
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT 3 IN (1, 2);
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 1 IN (1, NULL);
-- begin-expected
-- columns: ?column?
-- row: NULL
-- end-expected
SELECT 3 IN (1, NULL);
-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- end-expected
SELECT v FROM zz_ty WHERE v IN (10, 20) ORDER BY 1;
-- begin-expected
-- columns: t
-- row: a
-- row: b
-- end-expected
SELECT t FROM zz_ty WHERE t IN ('a', 'b') ORDER BY 1;

-- ============================================================================
-- An ANY or ALL settles both sides before it compares any of them
-- ============================================================================
-- The subquery's select list settles what it produces, whether or not it produces a row.
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 = ANY (SELECT 'x');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 = ANY (SELECT 'x' WHERE false);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 = ANY (SELECT '1');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 = ANY (SELECT NULL);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 = ANY (SELECT 'x'::text);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1.5 > ANY (SELECT 'x');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 < ALL (SELECT 'x');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 <> ANY (SELECT 'x');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT 1 >= ANY (SELECT 'x');
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT true = ANY (SELECT 1);
-- An untyped left side is read as what the subquery produces.
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 'a' = ANY (SELECT 1);
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '1' = ANY (SELECT 1);
-- Two sides of one family compare as they always did.
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT 1 = ANY (SELECT 1.5);
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT 1 = ANY (SELECT 1 WHERE false);
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 1 = ALL (SELECT 1 WHERE false);
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 10 = ANY (SELECT v FROM zz_ty);
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT 30 = ANY (SELECT v FROM zz_ty);
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 30 > ALL (SELECT v FROM zz_ty);

-- teardown
DROP TABLE IF EXISTS zz_ty CASCADE;
