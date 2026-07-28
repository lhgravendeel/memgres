-- How far the analysis reaches, and the order it reaches in.
--
-- Five things, all measured against PostgreSQL 18.
--
-- 1. A SQL function's body is analysed when the function is written. PostgreSQL parses and
--    analyses every statement in one at CREATE FUNCTION time, so a body that reads an ungrouped
--    column is refused there and not at the first call. The placement half of that check was
--    already here; the grouping half was not, and it needs the two things a running SELECT has
--    and a stored one does not: the select targets with stars expanded, and the relations the
--    FROM names. A PL/pgSQL body is not analysed — its statements are strings the PL handler
--    plans on first execution — and neither body is analysed when check_function_bodies is off.
--
-- 2. A partition bound may not read a column. A bound is a value the partition is given once,
--    with no row to read it from, so a name written in one is a column reference and PostgreSQL
--    says so — which is also why an enum label has to be quoted there. A bare name took the
--    plain-literal path in the bound parser and came back as a failed cast; only a multi-token
--    bound went through the expression path, which had no column check either.
--
-- 3. TABLE t is one of PostgreSQL's simple_select productions, alongside SELECT and VALUES, so it
--    is a query wherever a query may stand. Only the bare statement was read as one: a view body,
--    a set-operation arm, a CTE, a sub-select, an INSERT source and a cursor were all syntax
--    errors, and ORDER BY or LIMIT after one was a syntax error too.
--
-- 4. Which clause a query is refused for is the clause PostgreSQL reaches first, and it reads the
--    FROM clause, then the select list, then WHERE, then HAVING, the window definitions, ORDER BY,
--    GROUP BY, and last LIMIT and OFFSET. It is not "WHERE first": a bare window call in the
--    select list beats an aggregate in WHERE, because the select list is read first. What was
--    wrong was that the scan for a window call written without OVER ran over the whole query
--    ahead of the positional walk, so within WHERE it reported whichever it happened to reach.
--
-- 5. Both arms of a join name themselves to the query before its ON condition is read, so a name
--    given twice is reported ahead of anything the condition holds — while a misplaced call in ON
--    is still reported rather than evaluated.
--
-- The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches
-- too far is a refused valid statement.

-- setup
DROP VIEW IF EXISTS anr_v CASCADE;
DROP TABLE IF EXISTS anr_ins CASCADE;
DROP TABLE IF EXISTS anr_range CASCADE;
DROP TABLE IF EXISTS anr_list CASCADE;
DROP TABLE IF EXISTS anr_hash CASCADE;
DROP TABLE IF EXISTS anr_text CASCADE;
DROP TABLE IF EXISTS anr_u CASCADE;
DROP TABLE IF EXISTS anr_t CASCADE;

CREATE TABLE anr_t (id int PRIMARY KEY, a int, b text);
INSERT INTO anr_t VALUES (1, 10, 'x'), (2, 20, 'y');

CREATE TABLE anr_u (id int PRIMARY KEY, a int, c text);
INSERT INTO anr_u VALUES (1, 10, 'p'), (2, 30, 'q');

CREATE TABLE anr_range (id int, a int) PARTITION BY RANGE (id);
CREATE TABLE anr_list (id int, a int) PARTITION BY LIST (a);
CREATE TABLE anr_hash (id int, a int) PARTITION BY HASH (id);
CREATE TABLE anr_text (id int, b text) PARTITION BY RANGE (b);

-- ============================================================================
-- 1. A SQL function body is analysed when the function is created
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
CREATE FUNCTION anr_ungrouped() RETURNS text AS $$ SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql;

-- note: the relation the message names is the one the column reads, alias and all
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "x.b" must appear in the GROUP BY clause
-- end-expected-error
CREATE FUNCTION anr_ungrouped_alias() RETURNS text
    AS $$ SELECT x.b FROM anr_t x GROUP BY x.a $$ LANGUAGE sql;

-- note: HAVING makes a query grouped whether or not anything in it aggregates
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
CREATE FUNCTION anr_ungrouped_having() RETURNS text
    AS $$ SELECT b FROM anr_t GROUP BY a HAVING count(*) > 1 $$ LANGUAGE sql;

-- note: each arm of a set operation is a query of its own and is judged as one
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
CREATE FUNCTION anr_ungrouped_union() RETURNS text
    AS $$ SELECT b FROM anr_t UNION ALL SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql;

-- note: the same body written as a procedure, which PostgreSQL analyses the same way
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
CREATE PROCEDURE anr_ungrouped_proc() LANGUAGE sql AS $$ SELECT b FROM anr_t GROUP BY a $$;

-- note: SECURITY DEFINER changes who the body runs as, not whether it is analysed
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
CREATE FUNCTION anr_ungrouped_sd() RETURNS text
    AS $$ SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql SECURITY DEFINER;

-- note: a PL/pgSQL body is NOT analysed at CREATE time -- the query inside it is a string the
-- note: PL handler plans when the function runs, so this is accepted and fails when called
CREATE FUNCTION anr_plpgsql_ungrouped() RETURNS text LANGUAGE plpgsql
    AS $$ BEGIN RETURN (SELECT b FROM anr_t GROUP BY a); END $$;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT anr_plpgsql_ungrouped();

DROP FUNCTION anr_plpgsql_ungrouped();

-- note: with check_function_bodies off, nothing in either kind of body is analysed
SET check_function_bodies = off;

CREATE FUNCTION anr_unchecked() RETURNS text AS $$ SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql;

DROP FUNCTION anr_unchecked();
SET check_function_bodies = on;

-- ============================================================================
-- 2. A partition bound may not read a column
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
CREATE TABLE anr_r1 PARTITION OF anr_range FOR VALUES FROM (id) TO (10);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
CREATE TABLE anr_r2 PARTITION OF anr_range FOR VALUES FROM (0) TO (id);

-- note: a multi-token bound goes through the expression path, which is checked too
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
CREATE TABLE anr_r3 PARTITION OF anr_range FOR VALUES FROM (id + 1) TO (10);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
CREATE TABLE anr_r4 PARTITION OF anr_range FOR VALUES FROM (anr_range.id) TO (10);

-- note: a bound is settled once, so a sub-select has nothing to read either
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition bound
-- end-expected-error
CREATE TABLE anr_r5 PARTITION OF anr_range FOR VALUES FROM ((SELECT 1)) TO (10);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
CREATE TABLE anr_l1 PARTITION OF anr_list FOR VALUES IN (a);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
CREATE TABLE anr_l2 PARTITION OF anr_list FOR VALUES IN (1, id);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
CREATE TABLE anr_x1 PARTITION OF anr_text FOR VALUES FROM (b) TO ('m');

-- note: MODULUS and REMAINDER take an integer literal, so a name there is a syntax error
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "id"
-- end-expected-error
CREATE TABLE anr_h1 PARTITION OF anr_hash FOR VALUES WITH (MODULUS id, REMAINDER 0);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "4.5"
-- end-expected-error
CREATE TABLE anr_h2 PARTITION OF anr_hash FOR VALUES WITH (MODULUS 4.5, REMAINDER 0);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in partition bound expression
-- end-expected-error
ALTER TABLE anr_range ATTACH PARTITION anr_u FOR VALUES FROM (id) TO (10);

-- ---- every bound spelling that is a value keeps working ----

CREATE TABLE anr_ok1 PARTITION OF anr_range FOR VALUES FROM (0) TO (10);
DROP TABLE anr_ok1;
CREATE TABLE anr_ok2 PARTITION OF anr_range FOR VALUES FROM (MINVALUE) TO (0);
DROP TABLE anr_ok2;
CREATE TABLE anr_ok3 PARTITION OF anr_range FOR VALUES FROM (100) TO (MAXVALUE);
DROP TABLE anr_ok3;
CREATE TABLE anr_ok4 PARTITION OF anr_range FOR VALUES FROM (1 + 1) TO (10);
DROP TABLE anr_ok4;
CREATE TABLE anr_ok5 PARTITION OF anr_range FOR VALUES FROM (abs(-5)) TO (10);
DROP TABLE anr_ok5;
CREATE TABLE anr_ok6 PARTITION OF anr_range DEFAULT;
DROP TABLE anr_ok6;
CREATE TABLE anr_ok7 PARTITION OF anr_list FOR VALUES IN (1, 2);
DROP TABLE anr_ok7;
CREATE TABLE anr_ok8 PARTITION OF anr_list FOR VALUES IN (NULL);
DROP TABLE anr_ok8;
CREATE TABLE anr_ok9 PARTITION OF anr_list FOR VALUES IN (1 + 1);
DROP TABLE anr_ok9;
CREATE TABLE anr_ok10 PARTITION OF anr_list DEFAULT;
DROP TABLE anr_ok10;
CREATE TABLE anr_ok11 PARTITION OF anr_hash FOR VALUES WITH (MODULUS 4, REMAINDER 0);
DROP TABLE anr_ok11;
CREATE TABLE anr_ok12 PARTITION OF anr_text FOR VALUES FROM ('a') TO ('m');
DROP TABLE anr_ok12;
CREATE TABLE anr_ok13 PARTITION OF anr_text FOR VALUES FROM ('m'::text) TO ('z');
DROP TABLE anr_ok13;

-- ============================================================================
-- 3. TABLE t is a query wherever a query may stand
-- ============================================================================

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- end-expected
TABLE anr_t;

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- end-expected
TABLE anr_t ORDER BY id LIMIT 1;

CREATE VIEW anr_v AS TABLE anr_t;

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- end-expected
SELECT * FROM anr_v ORDER BY id;

DROP VIEW anr_v;

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- end-expected
SELECT id, a, b FROM anr_t UNION TABLE anr_t ORDER BY id;

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- end-expected
WITH w AS (TABLE anr_t) SELECT * FROM w ORDER BY id;

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- end-expected
SELECT * FROM (TABLE anr_t) s ORDER BY id;

CREATE TABLE anr_ins (id int, a int, b text);
INSERT INTO anr_ins TABLE anr_t;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM anr_ins;

DROP TABLE anr_ins;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS (TABLE anr_t) AS exists;

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- end-expected
TABLE anr_t EXCEPT TABLE anr_u ORDER BY id;

-- ============================================================================
-- 4. The clause a query is refused for is the one PostgreSQL reaches first
-- ============================================================================

-- note: the select list is only a star, so WHERE is read next and left to right --
-- note: count(*) stands before row_number(), and the aggregate is what is named
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM anr_t WHERE count(*) > 1 AND row_number() = 1;

-- note: the other way round, the select list is read first and its bare window call wins
-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT row_number() FROM anr_t WHERE count(*) > 1;

-- note: an aggregate in the select list is where an aggregate belongs, so WHERE decides
-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT count(*) FROM anr_t WHERE row_number() = 1;

-- note: a name in the select list that resolves to nothing is reached before WHERE
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT nosuchcol FROM anr_t WHERE row_number() = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT nosuchcol FROM anr_t WHERE count(*) > 1;

-- note: LIMIT is read last of all, so WHERE is what a query wrong in both is refused for
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM anr_t WHERE count(*) > 1 LIMIT count(*);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM anr_t WHERE count(*) > 1 GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM anr_t WHERE count(*) > 1 HAVING count(*) > 1;

-- ---- the clauses after WHERE still get judged when WHERE has nothing wrong ----

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT * FROM anr_t ORDER BY row_number();

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT * FROM anr_t GROUP BY row_number();

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT * FROM anr_t LIMIT row_number();

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in LIMIT
-- end-expected-error
SELECT * FROM anr_t LIMIT count(*);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in OFFSET
-- end-expected-error
SELECT * FROM anr_t OFFSET count(*);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in GROUP BY
-- end-expected-error
SELECT * FROM anr_t GROUP BY count(*);

-- ============================================================================
-- 5. A duplicate table name is reported before what the ON clause holds
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
SELECT * FROM anr_t x JOIN anr_u x ON count(*) = 1;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
SELECT * FROM anr_t x JOIN anr_u x ON row_number() = 1;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
SELECT * FROM anr_t x JOIN anr_u x ON nosuchcol = 1;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
SELECT * FROM anr_t x LEFT JOIN anr_u x ON true;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
SELECT * FROM anr_t x CROSS JOIN anr_u x;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "anr_t" specified more than once
-- end-expected-error
SELECT * FROM anr_t, anr_t;

-- note: with distinct names there is no clash, and the ON condition is judged as before
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in JOIN conditions
-- end-expected-error
SELECT * FROM anr_t x JOIN anr_u y ON count(*) = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in JOIN conditions
-- end-expected-error
SELECT * FROM anr_t x JOIN anr_u y ON count(*) OVER () = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT * FROM anr_t x JOIN anr_u y ON row_number() = 1;

-- note: a relation that does not exist is still reported before the duplicate name
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "anr_nosuch" does not exist
-- end-expected-error
SELECT * FROM anr_nosuch x JOIN anr_nosuch2 x ON true;

-- ============================================================================
-- Ordinary SQL, which has to keep working
-- ============================================================================

-- ---- function bodies that are perfectly ordinary queries ----

CREATE FUNCTION anr_f_grouped() RETURNS bigint
    AS $$ SELECT count(*) FROM anr_t GROUP BY a LIMIT 1 $$ LANGUAGE sql;
CREATE FUNCTION anr_f_windowed() RETURNS bigint
    AS $$ SELECT row_number() OVER (ORDER BY id) FROM anr_t LIMIT 1 $$ LANGUAGE sql;
CREATE FUNCTION anr_f_joined() RETURNS text
    AS $$ SELECT t.b FROM anr_t t JOIN anr_u u ON t.id = u.id LIMIT 1 $$ LANGUAGE sql;
CREATE FUNCTION anr_f_param(p int) RETURNS bigint
    AS $$ SELECT count(*) FROM anr_t WHERE a = p GROUP BY a $$ LANGUAGE sql;
CREATE FUNCTION anr_f_setof() RETURNS SETOF text AS $$ SELECT b FROM anr_t $$ LANGUAGE sql;
CREATE FUNCTION anr_f_table() RETURNS TABLE(z text) AS $$ SELECT b FROM anr_t $$ LANGUAGE sql;
CREATE FUNCTION anr_f_cte() RETURNS bigint
    AS $$ WITH w AS (SELECT a FROM anr_t) SELECT count(*) FROM w $$ LANGUAGE sql;
CREATE FUNCTION anr_f_catalog() RETURNS bigint
    AS $$ SELECT count(*) FROM pg_class $$ LANGUAGE sql;
CREATE FUNCTION anr_f_gsets() RETURNS bigint
    AS $$ SELECT count(*) FROM anr_t GROUP BY GROUPING SETS ((a), (b)) LIMIT 1 $$ LANGUAGE sql;
CREATE FUNCTION anr_f_selfjoin() RETURNS bigint
    AS $$ SELECT count(*) FROM anr_t t1, anr_t t2 WHERE t1.id = t2.id GROUP BY t1.a LIMIT 1 $$
    LANGUAGE sql;
CREATE FUNCTION anr_f_values() RETURNS bigint
    AS $$ SELECT count(*) FROM (VALUES (1), (2)) v(x) $$ LANGUAGE sql;
CREATE FUNCTION anr_f_srf() RETURNS bigint
    AS $$ SELECT count(*) FROM generate_series(1, 3) g $$ LANGUAGE sql;
CREATE FUNCTION anr_f_plpgsql() RETURNS bigint LANGUAGE plpgsql
    AS $$ BEGIN RETURN (SELECT count(*) FROM anr_t); END $$;

CREATE VIEW anr_v AS SELECT id, a, b FROM anr_t;
CREATE FUNCTION anr_f_view() RETURNS bigint AS $$ SELECT count(*) FROM anr_v $$ LANGUAGE sql;

-- begin-expected
-- columns: grouped | windowed | joined | param | cte | gsets | selfjoin | vals | srf | plp | vw
-- row: 1, 1, x, 1, 2, 1, 1, 2, 3, 2, 2
-- end-expected
SELECT anr_f_grouped() AS grouped, anr_f_windowed() AS windowed, anr_f_joined() AS joined,
       anr_f_param(10) AS param, anr_f_cte() AS cte, anr_f_gsets() AS gsets,
       anr_f_selfjoin() AS selfjoin, anr_f_values() AS vals, anr_f_srf() AS srf,
       anr_f_plpgsql() AS plp, anr_f_view() AS vw;

-- begin-expected
-- columns: z
-- row: x
-- row: y
-- end-expected
SELECT * FROM anr_f_setof() AS z ORDER BY z;

-- begin-expected
-- columns: z
-- row: x
-- row: y
-- end-expected
SELECT * FROM anr_f_table() ORDER BY z;

-- ---- joins with distinct aliases, self-joins, and no alias at all ----

-- begin-expected
-- columns: b | c
-- row: x, p
-- end-expected
SELECT t.b, u.c FROM anr_t t JOIN anr_u u ON t.a = u.a;

-- begin-expected
-- columns: l | r
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT x.id AS l, y.id AS r FROM anr_t x JOIN anr_t y ON x.id = y.id ORDER BY x.id;

-- begin-expected
-- columns: b | c
-- row: x, p
-- row: y, q
-- end-expected
SELECT anr_t.b, anr_u.c FROM anr_t JOIN anr_u ON anr_t.id = anr_u.id ORDER BY anr_t.id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT x.id FROM anr_t x JOIN (SELECT * FROM anr_u) y ON x.id = y.id ORDER BY x.id;

-- ---- WHERE clauses holding neither an aggregate nor a window call ----

-- begin-expected
-- columns: b
-- row: y
-- end-expected
SELECT b FROM anr_t WHERE a > 15;

-- begin-expected
-- columns: b
-- row: x
-- end-expected
SELECT b FROM anr_t WHERE a = (SELECT min(a) FROM anr_u);

-- begin-expected
-- columns: n
-- row: 2
-- row: 2
-- end-expected
SELECT count(*) OVER () AS n FROM anr_t WHERE a > 0;

-- begin-expected
-- columns: a | n
-- row: 10, 1
-- row: 20, 1
-- end-expected
SELECT a, count(*) AS n FROM anr_t WHERE a > 0 GROUP BY a HAVING count(*) > 0 ORDER BY a LIMIT 5;

-- cleanup
DROP VIEW IF EXISTS anr_v CASCADE;
DROP FUNCTION IF EXISTS anr_f_grouped();
DROP FUNCTION IF EXISTS anr_f_windowed();
DROP FUNCTION IF EXISTS anr_f_joined();
DROP FUNCTION IF EXISTS anr_f_param(int);
DROP FUNCTION IF EXISTS anr_f_setof();
DROP FUNCTION IF EXISTS anr_f_table();
DROP FUNCTION IF EXISTS anr_f_cte();
DROP FUNCTION IF EXISTS anr_f_catalog();
DROP FUNCTION IF EXISTS anr_f_gsets();
DROP FUNCTION IF EXISTS anr_f_selfjoin();
DROP FUNCTION IF EXISTS anr_f_values();
DROP FUNCTION IF EXISTS anr_f_srf();
DROP FUNCTION IF EXISTS anr_f_plpgsql();
DROP FUNCTION IF EXISTS anr_f_view();
DROP TABLE IF EXISTS anr_text CASCADE;
DROP TABLE IF EXISTS anr_hash CASCADE;
DROP TABLE IF EXISTS anr_list CASCADE;
DROP TABLE IF EXISTS anr_range CASCADE;
DROP TABLE IF EXISTS anr_u CASCADE;
DROP TABLE IF EXISTS anr_t CASCADE;
