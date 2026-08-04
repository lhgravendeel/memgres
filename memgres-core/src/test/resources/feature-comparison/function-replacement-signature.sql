-- What CREATE OR REPLACE may change about a routine, and what identifies one in the first place.
--
-- CREATE OR REPLACE keeps the routine's identity, so everything a caller was compiled against has
-- to survive it: whether it is a function or a procedure, the type a call yields, the names of the
-- input parameters, and which arguments a call may leave out. Change one of those and the result
-- is a different routine wearing the old one's name, which is why PostgreSQL makes you drop the
-- old one first.
--
-- What "the type a call yields" means is the subtle part, because RETURNS is not the whole story:
--
--  * A single output parameter IS the result. f(IN a int) RETURNS int and f(INOUT a int) are the
--    same function seen from outside, and either may replace the other. RETURNS TABLE(x int) is
--    RETURNS SETOF int for the same reason, and a lone output column may be renamed because a
--    scalar type has no column name.
--  * Two or more output parameters make a row type, and there the column names ARE part of it, so
--    renaming one is a changed return type.
--  * A procedure yields nothing at all, so what a caller sees is only whether values come back
--    through parameters -- and changing that has its own message. A procedure's single output
--    parameter is still a row, unlike a function's.
--
-- Also here: what identifies a function is the argument types, not the words they were written
-- with. int, int4 and integer are one type and a precision is not part of the type, so f(int) and
-- f(int4) are the same function -- registering both would leave no call able to choose between
-- them.

-- setup
DROP FUNCTION IF EXISTS frs_ret(int) CASCADE;
DROP FUNCTION IF EXISTS frs_out(int) CASCADE;
DROP FUNCTION IF EXISTS frs_row(int) CASCADE;
DROP FUNCTION IF EXISTS frs_tab(int) CASCADE;
DROP FUNCTION IF EXISTS frs_def(int) CASCADE;
DROP FUNCTION IF EXISTS frs_alias(int) CASCADE;

-- 1: a function never becomes a procedure, nor the other way round (42809, not 42P13)

CREATE FUNCTION frs_kind1(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change routine kind
-- end-expected-error
CREATE OR REPLACE PROCEDURE frs_kind1(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

CREATE FUNCTION frs_kind2(a int) RETURNS void AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change routine kind
-- end-expected-error
CREATE OR REPLACE PROCEDURE frs_kind2(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

CREATE PROCEDURE frs_kind3(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change routine kind
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_kind3(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- The kind is decided before anything else about the definition is read.

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change routine kind
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_kind3(b int) RETURNS text AS $$ SELECT 'x' $$ LANGUAGE sql;

CREATE PROCEDURE frs_kind4() AS $$ BEGIN END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change routine kind
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_kind4() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- A procedure replacing a procedure is ordinary.
CREATE OR REPLACE PROCEDURE frs_kind4() AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql;

-- 2: the declared return type

CREATE FUNCTION frs_ret(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_ret(a int) RETURNS text AS $$ SELECT 'x' $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_ret(a int) RETURNS SETOF int AS $$ SELECT 1 $$ LANGUAGE sql;

CREATE FUNCTION frs_ret2(a int) RETURNS varchar AS $$ SELECT 'x' $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_ret2(a int) RETURNS text AS $$ SELECT 'x' $$ LANGUAGE sql;

-- 3: a lone output parameter is the result, so it may move in and out of RETURNS

CREATE FUNCTION frs_out(IN a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_out(INOUT a int) AS $$ SELECT a + 10 $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_out
-- row: 11
-- end-expected
SELECT frs_out(1);

CREATE FUNCTION frs_out2(INOUT a int) AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_out2(IN a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- One of several parameters turning INOUT still leaves exactly one output.
CREATE FUNCTION frs_out3(IN a int, IN b int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_out3(INOUT a int, IN b int) AS $$ SELECT a $$ LANGUAGE sql;

-- An OUT parameter and a RETURNS of the same type are interchangeable.
CREATE FUNCTION frs_out4(a int, OUT r int) AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_out4(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

CREATE FUNCTION frs_out5(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_out5(a int, OUT r int) AS $$ SELECT a $$ LANGUAGE sql;

-- A scalar result carries no column name, so the lone output may be renamed.
CREATE FUNCTION frs_out6(a int, OUT r int) AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_out6(a int, OUT s int) AS $$ SELECT a + 1 $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_out6
-- row: 2
-- end-expected
SELECT frs_out6(1);

-- 4: two output parameters make a row whose column names are part of its type

CREATE FUNCTION frs_row(a int, OUT r int, OUT s int) AS $$ SELECT 1, 2 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_row(a int, OUT r int, OUT t int) AS $$ SELECT 1, 2 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_row(a int, OUT r int, OUT s text) AS $$ SELECT 1, 'x' $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_row(a int, OUT r int) AS $$ SELECT 1 $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION frs_row(a int, OUT r int, OUT s int) AS $$ SELECT 3, 4 $$ LANGUAGE sql;

CREATE FUNCTION frs_row2(IN a int, IN b int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_row2(INOUT a int, INOUT b int) AS $$ SELECT 1, 2 $$ LANGUAGE sql;

-- 5: RETURNS TABLE of one column is RETURNS SETOF of that column's type

CREATE FUNCTION frs_tab(a int) RETURNS TABLE(x int) AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_tab(a int) RETURNS SETOF int AS $$ SELECT a + 1 $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_tab
-- row: 2
-- end-expected
SELECT * FROM frs_tab(1);

CREATE FUNCTION frs_tab2(a int) RETURNS SETOF int AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_tab2(a int) RETURNS TABLE(x int) AS $$ SELECT a $$ LANGUAGE sql;

-- One column, so its name is not part of the type.
CREATE FUNCTION frs_tab3(a int) RETURNS TABLE(x int) AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_tab3(a int) RETURNS TABLE(y int) AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_tab3(a int) RETURNS TABLE(y text) AS $$ SELECT 'x' $$ LANGUAGE sql;

-- Two columns, so the names are.
CREATE FUNCTION frs_tab4(a int) RETURNS TABLE(x int, y int) AS $$ SELECT 1, 2 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_tab4(a int) RETURNS TABLE(p int, q int) AS $$ SELECT 1, 2 $$ LANGUAGE sql;

-- 6: a procedure is judged on whether it hands anything back

CREATE PROCEDURE frs_proc1(INOUT a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change whether a procedure has output parameters
-- end-expected-error
CREATE OR REPLACE PROCEDURE frs_proc1(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

CREATE PROCEDURE frs_proc2(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change whether a procedure has output parameters
-- end-expected-error
CREATE OR REPLACE PROCEDURE frs_proc2(INOUT a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

-- A procedure returns no value, so even a single output parameter is a row.

CREATE PROCEDURE frs_proc3(a int, OUT r int) AS $$ BEGIN r := 1; END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change return type of existing function
-- end-expected-error
CREATE OR REPLACE PROCEDURE frs_proc3(a int, OUT s int) AS $$ BEGIN s := 1; END $$ LANGUAGE plpgsql;

-- 7: an input parameter keeps its name

CREATE FUNCTION frs_name1(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change name of input parameter "a"
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_name1(b int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change name of input parameter "a"
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_name1(int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- An INOUT parameter is an input parameter; a single one leaves the return type alone, so it is
-- the name that is complained about.

CREATE FUNCTION frs_name2(INOUT a int) AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot change name of input parameter "a"
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_name2(INOUT b int) AS $$ SELECT b $$ LANGUAGE sql;

-- A parameter that never had a name has none to break.
CREATE FUNCTION frs_name3(int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_name3(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- 8: a default may not be taken away

CREATE FUNCTION frs_def(a int DEFAULT 1) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot remove parameter defaults from existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_def(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- The refused replacement changed nothing.

-- begin-expected
-- columns: frs_def
-- row: 1
-- end-expected
SELECT frs_def();

CREATE FUNCTION frs_def2(a int, b int DEFAULT 2) RETURNS int AS $$ SELECT b $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot remove parameter defaults from existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_def2(a int, b int) RETURNS int AS $$ SELECT b $$ LANGUAGE sql;

CREATE FUNCTION frs_def3(a int DEFAULT 1, b int DEFAULT 2) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot remove parameter defaults from existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_def3(a int, b int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- An unnamed parameter's default counts as much as a named one's.
CREATE FUNCTION frs_def4(int DEFAULT 1) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot remove parameter defaults from existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_def4(int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- So does a procedure's, and PostgreSQL says "function" either way.
CREATE PROCEDURE frs_def5(a int DEFAULT 1) AS $$ BEGIN END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot remove parameter defaults from existing function
-- end-expected-error
CREATE OR REPLACE PROCEDURE frs_def5(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql;

-- A default alongside an output parameter is still a default.
CREATE FUNCTION frs_def6(a int DEFAULT 1, OUT r int) AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: cannot remove parameter defaults from existing function
-- end-expected-error
CREATE OR REPLACE FUNCTION frs_def6(a int, OUT r int) AS $$ SELECT 1 $$ LANGUAGE sql;

-- 9: adding or changing a default is allowed -- more calls resolve, never fewer

CREATE FUNCTION frs_def7(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_def7(a int DEFAULT 8) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_def7
-- row: 8
-- end-expected
SELECT frs_def7();

CREATE FUNCTION frs_def8(a int DEFAULT 1) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_def8(a int DEFAULT 6) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_def8
-- row: 6
-- end-expected
SELECT frs_def8();

CREATE FUNCTION frs_def9(a int, b int DEFAULT 2) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_def9(a int DEFAULT 1, b int DEFAULT 2) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_def9
-- row: 3
-- end-expected
SELECT frs_def9();

-- 10: the argument types are the identity, not the words they were written with

CREATE FUNCTION frs_alias(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_alias(a int4) RETURNS int4 AS $$ SELECT 2 $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_alias
-- row: 2
-- end-expected
SELECT frs_alias(0);

CREATE FUNCTION frs_alias2(a varchar(5)) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_alias2(a character varying) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_alias2
-- row: 2
-- end-expected
SELECT frs_alias2('x');

CREATE FUNCTION frs_alias3(a numeric(10,2)) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42723
-- message-like: already exists with same argument types
-- end-expected-error
CREATE FUNCTION frs_alias3(a numeric) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;

CREATE FUNCTION frs_alias4(a int8) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_alias4(a bigint) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_alias4
-- row: 2
-- end-expected
SELECT frs_alias4(1);

-- Different types stay different functions.
CREATE FUNCTION frs_two(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_two(a text) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;

-- begin-expected
-- columns: i | t
-- row: 1, 2
-- end-expected
SELECT frs_two(1) AS i, frs_two('x') AS t;

CREATE FUNCTION frs_two2(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_two2(a bigint) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;

-- begin-expected
-- columns: i | b
-- row: 1, 2
-- end-expected
SELECT frs_two2(1::int) AS i, frs_two2(1::bigint) AS b;

-- A replacement that changes a parameter type is a new function, not a replacement.
CREATE FUNCTION frs_new(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_new(a text) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;

-- begin-expected
-- columns: i | t
-- row: 1, 2
-- end-expected
SELECT frs_new(1) AS i, frs_new('x') AS t;

-- 11: the defaults an application writes are accepted and used

CREATE FUNCTION frs_ok1(a int DEFAULT (1 + 2) * 3) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_ok1
-- row: 9
-- end-expected
SELECT frs_ok1();

CREATE FUNCTION frs_ok2(a int DEFAULT abs(-3)) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_ok2
-- row: 3
-- end-expected
SELECT frs_ok2();

CREATE FUNCTION frs_ok3(a int DEFAULT CASE WHEN true THEN 1 ELSE 2 END) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_ok3
-- row: 1
-- end-expected
SELECT frs_ok3();

CREATE FUNCTION frs_ok4(a int DEFAULT COALESCE(NULL, 4)) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_ok4
-- row: 4
-- end-expected
SELECT frs_ok4();

CREATE FUNCTION frs_ok5(a int DEFAULT (ARRAY[7,8])[1]) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_ok5
-- row: 7
-- end-expected
SELECT frs_ok5();

-- A string that happens to spell a query is a string.
CREATE FUNCTION frs_ok6(a text DEFAULT '(select 1)') RETURNS text AS $$ SELECT a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_ok6
-- row: (select 1)
-- end-expected
SELECT frs_ok6();

-- The niladic keyword functions are calls, not names.
CREATE FUNCTION frs_ok7(a date DEFAULT CURRENT_DATE) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok8(a text DEFAULT CURRENT_USER) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok9(a text DEFAULT USER) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok10(a timestamp DEFAULT LOCALTIMESTAMP) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok11(a text DEFAULT SESSION_USER) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok12(a interval DEFAULT INTERVAL '1 day') RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok13(a text DEFAULT 'x'::text) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok14(a bool DEFAULT true) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION frs_ok15(a int DEFAULT NULL) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_ok14
-- row: 1
-- end-expected
SELECT frs_ok14();

-- 12: an ordinary redefinition still replaces the body

CREATE FUNCTION frs_body(a text, b text) RETURNS text AS $$ SELECT a || b $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_body
-- row: xy
-- end-expected
SELECT frs_body('x', 'y');

CREATE OR REPLACE FUNCTION frs_body(a text, b text) RETURNS text AS $$ SELECT b || a $$ LANGUAGE sql;

-- begin-expected
-- columns: frs_body
-- row: yx
-- end-expected
SELECT frs_body('x', 'y');

-- Including one that changes language while keeping everything a caller can see.
CREATE FUNCTION frs_lang(a int DEFAULT 2) RETURNS int AS $$ SELECT a $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION frs_lang(a int DEFAULT 2) RETURNS int AS $$ BEGIN RETURN a * 3; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: frs_lang
-- row: 6
-- end-expected
SELECT frs_lang();

-- teardown: this file is the one that leaves routines with output parameters behind, and a later
-- file reads pg_proc as a whole, so it takes its own routines with it when it goes.

DROP ROUTINE IF EXISTS frs_kind1(int) CASCADE;
DROP ROUTINE IF EXISTS frs_kind2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_kind3(int) CASCADE;
DROP ROUTINE IF EXISTS frs_kind4() CASCADE;
DROP ROUTINE IF EXISTS frs_ret(int) CASCADE;
DROP ROUTINE IF EXISTS frs_ret2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_out(int) CASCADE;
DROP ROUTINE IF EXISTS frs_out2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_out3(int, int) CASCADE;
DROP ROUTINE IF EXISTS frs_out4(int) CASCADE;
DROP ROUTINE IF EXISTS frs_out5(int) CASCADE;
DROP ROUTINE IF EXISTS frs_out6(int) CASCADE;
DROP ROUTINE IF EXISTS frs_row(int) CASCADE;
DROP ROUTINE IF EXISTS frs_row2(int, int) CASCADE;
DROP ROUTINE IF EXISTS frs_tab(int) CASCADE;
DROP ROUTINE IF EXISTS frs_tab2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_tab3(int) CASCADE;
DROP ROUTINE IF EXISTS frs_tab4(int) CASCADE;
DROP ROUTINE IF EXISTS frs_proc1(int) CASCADE;
DROP ROUTINE IF EXISTS frs_proc2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_proc3(int) CASCADE;
DROP ROUTINE IF EXISTS frs_name1(int) CASCADE;
DROP ROUTINE IF EXISTS frs_name2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_name3(int) CASCADE;
DROP ROUTINE IF EXISTS frs_def(int) CASCADE;
DROP ROUTINE IF EXISTS frs_def2(int, int) CASCADE;
DROP ROUTINE IF EXISTS frs_def3(int, int) CASCADE;
DROP ROUTINE IF EXISTS frs_def4(int) CASCADE;
DROP ROUTINE IF EXISTS frs_def5(int) CASCADE;
DROP ROUTINE IF EXISTS frs_def6(int) CASCADE;
DROP ROUTINE IF EXISTS frs_def7(int) CASCADE;
DROP ROUTINE IF EXISTS frs_def8(int) CASCADE;
DROP ROUTINE IF EXISTS frs_def9(int, int) CASCADE;
DROP ROUTINE IF EXISTS frs_alias(int) CASCADE;
DROP ROUTINE IF EXISTS frs_alias2(varchar) CASCADE;
DROP ROUTINE IF EXISTS frs_alias3(numeric) CASCADE;
DROP ROUTINE IF EXISTS frs_alias4(bigint) CASCADE;
DROP ROUTINE IF EXISTS frs_two(int) CASCADE;
DROP ROUTINE IF EXISTS frs_two(text) CASCADE;
DROP ROUTINE IF EXISTS frs_two2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_two2(bigint) CASCADE;
DROP ROUTINE IF EXISTS frs_new(int) CASCADE;
DROP ROUTINE IF EXISTS frs_new(text) CASCADE;
DROP ROUTINE IF EXISTS frs_ok1(int) CASCADE;
DROP ROUTINE IF EXISTS frs_ok2(int) CASCADE;
DROP ROUTINE IF EXISTS frs_ok3(int) CASCADE;
DROP ROUTINE IF EXISTS frs_ok4(int) CASCADE;
DROP ROUTINE IF EXISTS frs_ok5(int) CASCADE;
DROP ROUTINE IF EXISTS frs_ok6(text) CASCADE;
DROP ROUTINE IF EXISTS frs_ok7(date) CASCADE;
DROP ROUTINE IF EXISTS frs_ok8(text) CASCADE;
DROP ROUTINE IF EXISTS frs_ok9(text) CASCADE;
DROP ROUTINE IF EXISTS frs_ok10(timestamp) CASCADE;
DROP ROUTINE IF EXISTS frs_ok11(text) CASCADE;
DROP ROUTINE IF EXISTS frs_ok12(interval) CASCADE;
DROP ROUTINE IF EXISTS frs_ok13(text) CASCADE;
DROP ROUTINE IF EXISTS frs_ok14(bool) CASCADE;
DROP ROUTINE IF EXISTS frs_ok15(int) CASCADE;
DROP ROUTINE IF EXISTS frs_body(text, text) CASCADE;
DROP ROUTINE IF EXISTS frs_lang(int) CASCADE;
