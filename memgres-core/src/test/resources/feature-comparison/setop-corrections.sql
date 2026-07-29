-- What a set operation and a subquery may be written with.
--
-- Six things, all measured against PostgreSQL 18.
--
-- 1. The ORDER BY of a set operation takes an output column, and a cast to the type that column
--    already has is still that column. "Only a result column name" is the shape of what
--    PostgreSQL accepts, not the test it applies: it analyses the item and then looks for the
--    result among the output columns it already has, refusing only when the item added one. A
--    cast to the same type is a relabel PostgreSQL elides, so ORDER BY a::int over an integer a
--    sorts -- while a::bigint, b::varchar over text, +a, abs(a) and 1::int are all refused.
--    Refusing every cast alike refused SQL PostgreSQL runs. The refusal carries a Detail and a
--    Hint, which were missing.
--
-- 2. A name that reaches two FROM entries reaches neither. FROM s1.t, s2.t is legal -- either can
--    still be reached by writing its schema -- so PostgreSQL admits the FROM clause and then
--    calls every bare t.n in it ambiguous. Taking the first match answered from one of them; and
--    because the parser dropped the schema from s2.t.*, that star expanded both relations, and a
--    star qualified by a schema that does not hold the relation expanded it anyway.
--
-- 3. Both sides of a comparison against a subquery have to be the same width, and that is settled
--    from the two select lists before any row is read. Only the narrow half was checked, so
--    1 IN (SELECT 1, 2) compared against the first column and answered true. A side whose width
--    the query text does not fix is still not judged: x in FROM t x is a whole row, and
--    x IN (SELECT y FROM t y) is a comparison PostgreSQL makes.
--
-- 4. ORDER BY, LIMIT, OFFSET and FOR UPDATE belong to the set operation and not to an arm, so
--    PostgreSQL's grammar has no production for one on an unparenthesised arm. Parsing the arms
--    as ordinary SELECTs and moving the clauses up afterwards accepted all four: the ORDER BY
--    silently applied to the union, and the LIMIT silently applied to the first arm alone.
--    Parenthesised they are legal and mean the arm, which is the difference the check turns on.
--
-- 5. A prepared body is analysed when it is prepared, not when it is executed, so a FROM clause
--    naming one relation twice and a parameter in a set operation's ORDER BY are both refused at
--    PREPARE.
--
-- 6. A name may be written with four parts. PostgreSQL reaches the catalog it is connected to and
--    refuses any other as a cross-database reference; a fourth part was a syntax error at the dot.
--
-- The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches
-- too far is a refused valid statement.

-- setup
DROP VIEW IF EXISTS stc_wv CASCADE;
DROP TABLE IF EXISTS stc_s1.tw CASCADE;
DROP TABLE IF EXISTS stc_s2.tw CASCADE;
DROP SCHEMA IF EXISTS stc_s1 CASCADE;
DROP SCHEMA IF EXISTS stc_s2 CASCADE;
DROP TABLE IF EXISTS stc_w1 CASCADE;
DROP TABLE IF EXISTS stc_t1 CASCADE;
DROP TABLE IF EXISTS stc_dpt CASCADE;
DROP TABLE IF EXISTS stc_emp CASCADE;
DROP TABLE IF EXISTS stc_a CASCADE;
DROP TABLE IF EXISTS stc_p CASCADE;
DROP TABLE IF EXISTS stc_tb CASCADE;
DROP TABLE IF EXISTS stc_proj CASCADE;
DEALLOCATE ALL;

CREATE SCHEMA stc_s1;
CREATE SCHEMA stc_s2;
CREATE TABLE stc_s1.tw (n int, m int);
CREATE TABLE stc_s2.tw (n int);
INSERT INTO stc_s1.tw VALUES (1, 11);
INSERT INTO stc_s2.tw VALUES (2);

CREATE TABLE stc_w1 (a int PRIMARY KEY, b text);
INSERT INTO stc_w1 VALUES (1, 'x'), (2, 'y');

CREATE TABLE stc_t1 (a int, b text);
INSERT INTO stc_t1 VALUES (1, 'x'), (2, 'y'), (3, 'z');

CREATE TABLE stc_dpt (id int PRIMARY KEY, name text);
INSERT INTO stc_dpt VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

CREATE TABLE stc_emp (id int PRIMARY KEY, dept_id int, name text);
INSERT INTO stc_emp VALUES (1, 1, 'e'), (5, 2, 'f');

CREATE TABLE stc_a (id int, v int);
INSERT INTO stc_a VALUES (1, 10);

CREATE TABLE stc_p (id int PRIMARY KEY, v int);
INSERT INTO stc_p VALUES (1, 10), (2, 20);

CREATE TABLE stc_tb (c int, d text);
CREATE TABLE stc_proj (id int PRIMARY KEY, code text);

-- ============================================================================
-- 1. A set operation's ORDER BY takes an output column
-- ============================================================================

-- note: a is integer already, so ::int adds nothing and the item is the column
-- expected: 1, 2, 9
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int;

-- expected: 1, 2, 9
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int4;

-- expected: 1, 2, 9
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY CAST(a AS int);

-- note: two no-op casts in a row are still no casts at all
-- expected: 1, 2, 9
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::integer::int;

-- expected: 9, 2, 1
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int DESC;

-- expected: 1, 2
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int LIMIT 2;

-- expected: 1, 2
SELECT a FROM stc_w1 EXCEPT SELECT 9 ORDER BY a::int;

-- expected: q, x, y
SELECT b FROM stc_w1 UNION SELECT 'q' ORDER BY b::text;

-- expected: (9,q), (1,x), (2,y)
SELECT a, b FROM stc_w1 UNION SELECT 9, 'q' ORDER BY b::text, a::int;

-- note: a real coercion is an expression the clause does not take
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- detail-like: Only result column names can be used, not expressions or functions.
-- hint-like: Add the expression/function to every SELECT, or move the UNION into a FROM clause.
-- end-expected-error
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::bigint;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::numeric;

-- note: text and character varying are two types, so this is a coercion too
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT b FROM stc_w1 UNION SELECT 'q' ORDER BY b::varchar;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY +a;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY abs(a);

-- note: a constant cast to its own type is a constant, not an output column
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY 1::int;

-- note: COLLATE is a node of its own and never peels away, even over a no-op cast
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT b FROM stc_w1 UNION SELECT 'q' ORDER BY b::text COLLATE "C";

-- note: a collation asked of an integer is still the collation complaint
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a COLLATE "C";

-- note: a name written under a cast is looked up before the clause is judged as a whole
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY nosuchcol::int;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- detail-like: Only result column names can be used, not expressions or functions.
-- hint-like: Add the expression/function to every SELECT, or move the UNION into a FROM clause.
-- end-expected-error
SELECT id FROM stc_dpt UNION SELECT id FROM stc_emp ORDER BY id + 0;

-- ============================================================================
-- 2. A name that reaches two FROM entries reaches neither
-- ============================================================================

-- note: the FROM clause itself is legal; it is the bare qualifier that is not
-- begin-expected-error
-- sqlstate: 42P09
-- message-like: table reference "tw" is ambiguous
-- end-expected-error
SELECT tw.n FROM stc_s1.tw, stc_s2.tw;

-- begin-expected-error
-- sqlstate: 42P09
-- message-like: table reference "tw" is ambiguous
-- end-expected-error
SELECT tw.n FROM stc_s2.tw, stc_s1.tw;

-- begin-expected-error
-- sqlstate: 42P09
-- message-like: table reference "tw" is ambiguous
-- end-expected-error
SELECT tw.* FROM stc_s1.tw, stc_s2.tw;

-- begin-expected-error
-- sqlstate: 42P09
-- message-like: table reference "tw" is ambiguous
-- end-expected-error
SELECT 1 FROM stc_s1.tw JOIN stc_s2.tw ON tw.n = 1;

-- note: a view body is analysed when it is written, so the same refusal lands there
-- begin-expected-error
-- sqlstate: 42P09
-- message-like: table reference "tw" is ambiguous
-- end-expected-error
CREATE VIEW stc_wv AS SELECT tw.n FROM stc_s1.tw, stc_s2.tw;

-- note: the schema picks the entry out, and the star stands for that one relation
-- expected: 2
SELECT stc_s2.tw.* FROM stc_s1.tw, stc_s2.tw ORDER BY 1;

-- expected: (1,11)
SELECT stc_s1.tw.* FROM stc_s1.tw, stc_s2.tw ORDER BY 1;

-- expected: 2
SELECT stc_s2.tw.n FROM stc_s1.tw, stc_s2.tw ORDER BY 1;

-- note: a schema that does not hold the relation reaches nothing
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "tw"
-- detail-like: There is an entry for table "tw", but it cannot be referenced from this part of the query.
-- end-expected-error
SELECT stc_s1.tw.* FROM stc_s2.tw;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "stc_a"
-- end-expected-error
SELECT nosuchschema.stc_a.* FROM stc_a;

-- note: an alias hides the relation's own name from the star as well as from a column
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "stc_a"
-- hint-like: Perhaps you meant to reference the table alias "a".
-- end-expected-error
SELECT public.stc_a.* FROM stc_a a;

-- ============================================================================
-- 3. Both sides of a subquery comparison are one width
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too many columns
-- end-expected-error
SELECT 1 WHERE 1 IN (SELECT 1, 2);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too many columns
-- end-expected-error
SELECT 1 WHERE 1 NOT IN (SELECT 1, 2);

-- note: the widths are a question about the select lists, so a null left side is no answer
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too many columns
-- end-expected-error
SELECT 1 WHERE NULL IN (SELECT 1, 2);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too many columns
-- end-expected-error
SELECT 1 = ANY (SELECT id, name FROM stc_dpt);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too many columns
-- end-expected-error
SELECT 1 = ALL (SELECT id, name FROM stc_dpt);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too few columns
-- end-expected-error
SELECT 1 WHERE (1,2) IN (SELECT 1);

-- note: SELECT with an empty select list is a query with no column at all
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT (SELECT);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ARRAY(SELECT);

-- ============================================================================
-- 4. The trailing clauses belong to the set operation
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near
-- end-expected-error
SELECT a FROM stc_t1 ORDER BY 1 UNION SELECT 5;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near
-- end-expected-error
SELECT a FROM stc_t1 LIMIT 1 UNION SELECT 5;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near
-- end-expected-error
SELECT a FROM stc_t1 OFFSET 1 UNION SELECT 5;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near
-- end-expected-error
SELECT a FROM stc_t1 ORDER BY 1 INTERSECT SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near
-- end-expected-error
SELECT a FROM stc_t1 ORDER BY 1 EXCEPT SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near
-- end-expected-error
SELECT id FROM stc_dpt FOR UPDATE UNION SELECT id FROM stc_emp;

-- note: written after the whole set operation the lock is refused rather than dropped
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT
-- end-expected-error
SELECT id FROM stc_dpt UNION SELECT id FROM stc_emp ORDER BY 1 FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR SHARE is not allowed with UNION/INTERSECT/EXCEPT
-- end-expected-error
SELECT id FROM stc_dpt UNION SELECT id FROM stc_emp FOR SHARE;

-- note: USING takes an ordering operator, and these four order nothing
-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator <= is not a valid ordering operator
-- hint-like: Ordering operators must be "<" or ">" members of btree operator families.
-- end-expected-error
SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1 USING <=;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator >= is not a valid ordering operator
-- end-expected-error
SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1 USING >=;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator = is not a valid ordering operator
-- end-expected-error
SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1 USING =;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator <> is not a valid ordering operator
-- end-expected-error
SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1 USING <>;

-- ============================================================================
-- 5. A prepared body is analysed when it is prepared
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "a" specified more than once
-- end-expected-error
PREPARE stc_dp AS SELECT 1 FROM stc_tb a, stc_tb a;

-- note: a parameter is the one item that can never be an output column name
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- detail-like: Only result column names can be used, not expressions or functions.
-- end-expected-error
PREPARE stc_p2(int) AS SELECT 1 AS n UNION SELECT 2 ORDER BY $1;

-- ============================================================================
-- 6. A name may be written with four parts
-- ============================================================================

-- note: naming the session catalog reaches it, and naming any other does not. The
-- positive half cannot be written here because the two engines are connected to
-- databases of different names and this file is replayed verbatim against both;
-- SetOpCorrectionTest asserts it against whatever current_database() answers.

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cross-database references are not implemented: nosuchdb.public.stc_dpt.id
-- end-expected-error
SELECT nosuchdb.public.stc_dpt.id FROM public.stc_dpt;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cross-database references are not implemented: nosuchdb.public.stc_dpt.*
-- end-expected-error
SELECT nosuchdb.public.stc_dpt.* FROM public.stc_dpt;

-- ============================================================================
-- 7. Messages that name what PostgreSQL names
-- ============================================================================

-- note: "integer", not the catalog's "int4", the way the mismatch error beside it already does
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT 1 UNION SELECT 'abc';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT 1 UNION ALL SELECT 'abc';

-- note: a target and a FROM item of one name are given twice, not ambiguous
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "stc_proj" specified more than once
-- end-expected-error
UPDATE stc_proj SET code = code FROM stc_proj WHERE id = -1;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "stc_proj" specified more than once
-- end-expected-error
DELETE FROM stc_proj USING stc_proj WHERE id = -1;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "p" specified more than once
-- end-expected-error
UPDATE stc_proj p SET code = code FROM stc_a p WHERE p.id = -1;

-- ============================================================================
-- 8. Ordinary SQL, which has to keep working
-- ============================================================================

-- expected: 1, 2, 3, 5
SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1;

-- expected: 1, 2
SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1 LIMIT 2;

-- expected: 2, 3, 5
SELECT a FROM stc_t1 UNION ALL SELECT 5 ORDER BY 1 OFFSET 1;

-- expected: 1
SELECT a FROM stc_t1 INTERSECT SELECT 1 ORDER BY 1;

-- expected: 2, 3
SELECT a FROM stc_t1 EXCEPT SELECT 1 ORDER BY 1;

-- expected: 1, 2, 9
SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a;

-- expected: 1, 2, 9
SELECT a AS z FROM stc_w1 UNION SELECT 9 ORDER BY z;

-- expected: 1, 2, 9
SELECT a FROM stc_w1 UNION ALL SELECT 9 ORDER BY (a);

-- note: parenthesised, all of these belong to the arm and are legal there
-- expected: 1, 1
(SELECT id FROM stc_dpt LIMIT 1) UNION ALL (SELECT id FROM stc_emp LIMIT 1) ORDER BY 1;

-- expected: 1, 2, 9
WITH q AS (SELECT a FROM stc_t1 ORDER BY 1) SELECT a FROM q WHERE a < 3 UNION SELECT 9 ORDER BY 1;

-- expected: 1, 2
SELECT a FROM stc_t1 WHERE a IN (SELECT a FROM stc_t1 ORDER BY 1 LIMIT 2) ORDER BY 1;

-- note: < and > are the operators that do order
-- expected: 1, 2, 3
SELECT a FROM stc_t1 ORDER BY a USING <;

-- expected: 3, 2, 1
SELECT a FROM stc_t1 ORDER BY a USING >;

-- expected: x, y, z
SELECT b FROM stc_t1 ORDER BY b USING <;

-- expected: 1, 2, 5
SELECT a FROM stc_t1 WHERE a < 3 UNION SELECT 5 ORDER BY 1 USING <;

-- expected: 1
SELECT 1 WHERE 1 IN (SELECT 1);

-- expected: 1
SELECT 1 WHERE (1,2) IN (SELECT 1, 2);

-- expected: 1
SELECT 1 WHERE ROW(1,2) IN (SELECT 1, 2);

-- expected: 1
SELECT 1 WHERE (SELECT 1) IN (SELECT 1);

-- expected: 1
SELECT 1 WHERE 1 IN (SELECT id FROM stc_dpt);

-- expected: (1,10), (2,20)
SELECT * FROM stc_p WHERE (id,v) IN (SELECT id,v FROM stc_p) ORDER BY 1;

-- note: x is the whole row of stc_p and so is y -- one column each, whatever their width
-- expected: 2
SELECT count(*) FROM stc_p x WHERE x IN (SELECT y FROM stc_p y);

-- note: EXISTS asks only whether a row came back, so the subquery's width is nothing to it
-- expected: t
SELECT EXISTS (SELECT 1, 2);

-- expected: 1
SELECT (SELECT 1);

-- expected: {1,2,3,4}
SELECT ARRAY(SELECT id FROM stc_dpt ORDER BY 1);

-- note: two relations of one name, each reached by its own schema
-- expected: 1
SELECT tw.n FROM stc_s1.tw;

-- expected: 1
SELECT tw.n FROM stc_s1.tw, stc_s2.tw x;

-- expected: (1,11,2)
SELECT * FROM stc_s1.tw, stc_s2.tw;

-- expected: (1,10)
SELECT public.stc_a.* FROM stc_a;

-- expected: (1,10)
SELECT public.stc_a.* FROM public.stc_a;

-- expected: (1,10)
SELECT stc_a.* FROM stc_a;

-- note: prepared bodies that analyse cleanly
PREPARE stc_ok1(int) AS SELECT $1 AS n UNION SELECT 2 ORDER BY 1;
PREPARE stc_ok2(int) AS SELECT id FROM stc_dpt WHERE id > $1 ORDER BY $1;
PREPARE stc_ok3 AS SELECT id FROM stc_dpt UNION SELECT id FROM stc_emp ORDER BY id;
PREPARE stc_ok4 AS SELECT 1 FROM stc_tb a, stc_tb b;
PREPARE stc_ok5 AS SELECT 1 FROM stc_tb, stc_tb b;
PREPARE stc_ok6 AS SELECT 1 FROM stc_s1.tw, stc_s2.tw x;
PREPARE stc_ok7 AS WITH q AS (SELECT 1 AS z) SELECT z FROM q;

-- expected: 2, 3, 4
EXECUTE stc_ok2(1);

-- cleanup
DEALLOCATE ALL;
DROP TABLE IF EXISTS stc_proj CASCADE;
DROP TABLE IF EXISTS stc_tb CASCADE;
DROP TABLE IF EXISTS stc_p CASCADE;
DROP TABLE IF EXISTS stc_a CASCADE;
DROP TABLE IF EXISTS stc_emp CASCADE;
DROP TABLE IF EXISTS stc_dpt CASCADE;
DROP TABLE IF EXISTS stc_t1 CASCADE;
DROP TABLE IF EXISTS stc_w1 CASCADE;
DROP SCHEMA IF EXISTS stc_s2 CASCADE;
DROP SCHEMA IF EXISTS stc_s1 CASCADE;
