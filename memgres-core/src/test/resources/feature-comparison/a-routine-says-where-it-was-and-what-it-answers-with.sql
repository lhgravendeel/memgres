-- ============================================================================
-- -- A routine says where it was, and answers with what it promised.
-- --
-- -- An error raised inside a body is reported with the frames the body was inside: the
-- -- expression or the SQL that failed, and the line of the body it was written on. What comes
-- -- back out is read as the type the routine was declared to return, a set is checked column by
-- -- column against the shape it promised, and a composite answers with its fields. Inside, a
-- -- loop counts in the range its type has and stops where its bound does, EXIT and CONTINUE are
-- -- not exceptions for a handler to catch, a handler names only conditions that are errors, and
-- -- a declaration is a type the grammar can read.
--
-- ============================================================================

-- ============================================================================
-- 1. An error names where in the body it was raised
-- ============================================================================
CREATE TABLE zzr_t(id int, nm text);
INSERT INTO zzr_t VALUES (1, 'a'), (2, 'b');
CREATE FUNCTION zzr_assign() RETURNS int LANGUAGE plpgsql AS $$
DECLARE v int;
BEGIN
  v := 1/0;
  RETURN v;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_assign() AS a;
CREATE FUNCTION zzr_sql() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO zzr_t VALUES (1/0);
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_sql() AS a;
CREATE FUNCTION zzr_perform() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1/0;
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_perform() AS a;
CREATE FUNCTION zzr_if() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  IF 1/0 = 1 THEN RETURN 2; END IF;
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_if() AS a;
CREATE FUNCTION zzr_while() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  WHILE 1/0 = 1 LOOP NULL; END LOOP;
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_while() AS a;
CREATE FUNCTION zzr_forq() RETURNS int LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT 1/0 LOOP NULL; END LOOP;
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_forq() AS a;
CREATE FUNCTION zzr_exec() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE 'SELECT 1/0';
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_exec() AS a;
CREATE FUNCTION zzr_return() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RETURN 1/0;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_return() AS a;
CREATE FUNCTION zzr_rq() RETURNS SETOF int LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT 1/0;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT * FROM zzr_rq();
CREATE FUNCTION zzr_rn() RETURNS SETOF int LANGUAGE plpgsql AS $$
BEGIN
  RETURN NEXT 1/0;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT * FROM zzr_rn();
CREATE FUNCTION zzr_case() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  CASE 1/0 WHEN 1 THEN RETURN 1; ELSE RETURN 2; END CASE;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_case() AS a;
CREATE FUNCTION zzr_open() RETURNS int LANGUAGE plpgsql AS $$
DECLARE c refcursor;
BEGIN
  OPEN c FOR SELECT 1/0;
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_open() AS a;
CREATE FUNCTION zzr_exit() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0;
BEGIN
  LOOP
    n := n + 1;
    EXIT WHEN 1/0 = 1;
  END LOOP;
  RETURN n;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_exit() AS a;
CREATE FUNCTION zzr_foreach() RETURNS int LANGUAGE plpgsql AS $$
DECLARE x int;
BEGIN
  FOREACH x IN ARRAY ARRAY[1,2] LOOP
    x := 1/0;
  END LOOP;
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_foreach() AS a;
CREATE FUNCTION zzr_nested() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    RETURN 1/0;
  END;
END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zzr_nested() AS a;
CREATE FUNCTION zzr_raise() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'e %', 1 USING HINT = 'h', DETAIL = 'd';
END $$;
-- begin-expected-error
-- sqlstate: P0001
-- message-like: e 1
-- end-expected-error
SELECT zzr_raise() AS a;
CREATE FUNCTION zzr_assert() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  ASSERT 1 = 2, 'custom ' || 5;
  RETURN 1;
END $$;
-- begin-expected-error
-- sqlstate: P0004
-- message-like: custom 5
-- end-expected-error
SELECT zzr_assert() AS a;
CREATE FUNCTION zzr_cast() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RETURN 'notanint';
END $$;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "notanint"
-- end-expected-error
SELECT zzr_cast() AS a;
CREATE FUNCTION zzr_nodest() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  SELECT 1;
  RETURN 2;
END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: query has no destination for result data
-- end-expected-error
SELECT zzr_nodest() AS a;
-- ============================================================================
-- 2. A loop counts in the range the type has, and ends where its bound does
-- ============================================================================
CREATE FUNCTION zzr_last() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0;
BEGIN FOR i IN 2147483645..2147483647 LOOP n := n + 1; END LOOP; RETURN n; END $$;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT zzr_last() AS a;
CREATE FUNCTION zzr_wide() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0;
BEGIN FOR i IN 1..2147483648 LOOP n := n + 1; EXIT WHEN n > 2; END LOOP; RETURN n; END $$;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT zzr_wide() AS a;
CREATE FUNCTION zzr_lonull() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0; lo int := NULL;
BEGIN FOR i IN lo..3 LOOP n := n + 1; END LOOP; RETURN n; END $$;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: lower bound of FOR loop cannot be null
-- end-expected-error
SELECT zzr_lonull() AS a;
CREATE FUNCTION zzr_hinull() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0; hi int := NULL;
BEGIN FOR i IN 1..hi LOOP n := n + 1; END LOOP; RETURN n; END $$;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: upper bound of FOR loop cannot be null
-- end-expected-error
SELECT zzr_hinull() AS a;
CREATE FUNCTION zzr_stnull() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0; st int := NULL;
BEGIN FOR i IN 1..3 BY st LOOP n := n + 1; END LOOP; RETURN n; END $$;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: BY value of FOR loop cannot be null
-- end-expected-error
SELECT zzr_stnull() AS a;
CREATE FUNCTION zzr_bystep() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0;
BEGIN FOR i IN 1..10 BY 3 LOOP n := n + i; END LOOP; RETURN n; END $$;
-- begin-expected
-- columns: a
-- row: 22
-- end-expected
SELECT zzr_bystep() AS a;
CREATE FUNCTION zzr_byzero() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0;
BEGIN FOR i IN 1..3 BY 0 LOOP n := n + i; END LOOP; RETURN n; END $$;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: BY value of FOR loop must be greater than zero
-- end-expected-error
SELECT zzr_byzero() AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: missing expression at or near ";"
-- end-expected-error
CREATE FUNCTION zzr_emptywhen() RETURNS int LANGUAGE plpgsql AS $$
BEGIN LOOP EXIT WHEN; END LOOP; RETURN 1; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: missing expression at or near ";"
-- end-expected-error
CREATE FUNCTION zzr_emptycont() RETURNS int LANGUAGE plpgsql AS $$
BEGIN LOOP CONTINUE WHEN; END LOOP; RETURN 1; END $$;
-- ============================================================================
-- 3. EXIT and CONTINUE are not exceptions, and a handler does not catch them
-- ============================================================================
CREATE FUNCTION zzr_exitcross() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0;
BEGIN
  FOR i IN 1..10 LOOP
    BEGIN
      IF i = 3 THEN EXIT; END IF;
      n := n + 1;
    EXCEPTION WHEN OTHERS THEN n := -100;
    END;
  END LOOP;
  RETURN n;
END $$;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT zzr_exitcross() AS a;
CREATE FUNCTION zzr_contcross() RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int := 0;
BEGIN
  FOR i IN 1..5 LOOP
    BEGIN
      IF i = 3 THEN CONTINUE; END IF;
      n := n + 1;
    EXCEPTION WHEN OTHERS THEN n := -100;
    END;
  END LOOP;
  RETURN n;
END $$;
-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT zzr_contcross() AS a;
CREATE FUNCTION zzr_retcross() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    RETURN 7;
  EXCEPTION WHEN OTHERS THEN RETURN -1;
  END;
END $$;
-- begin-expected
-- columns: a
-- row: 7
-- end-expected
SELECT zzr_retcross() AS a;
-- ============================================================================
-- 4. A handler names conditions the server knows, and only errors are conditions
-- ============================================================================
CREATE FUNCTION zzr_category() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    RAISE EXCEPTION 'x' USING ERRCODE = '22012';
  EXCEPTION WHEN data_exception THEN RETURN 'category';
  END;
END $$;
-- begin-expected
-- columns: a
-- row: category
-- end-expected
SELECT zzr_category() AS a;
CREATE FUNCTION zzr_others_or() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    RAISE EXCEPTION 'x' USING ERRCODE = 'P0002';
  EXCEPTION WHEN OTHERS OR division_by_zero THEN RETURN 'others-first';
  END;
END $$;
-- begin-expected
-- columns: a
-- row: others-first
-- end-expected
SELECT zzr_others_or() AS a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized exception condition "no_data"
-- end-expected-error
CREATE FUNCTION zzr_nodata() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    RAISE EXCEPTION 'x' USING ERRCODE = '22012';
  EXCEPTION WHEN no_data OR division_by_zero THEN RETURN 'or-list';
  END;
END $$;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized exception condition "warning"
-- end-expected-error
CREATE FUNCTION zzr_warning() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  BEGIN RETURN 'x'; EXCEPTION WHEN warning THEN RETURN 'y'; END;
END $$;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized exception condition "successful_completion"
-- end-expected-error
CREATE FUNCTION zzr_success() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  BEGIN RETURN 'x'; EXCEPTION WHEN successful_completion THEN RETURN 'y'; END;
END $$;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized exception condition "no_such_condition"
-- end-expected-error
CREATE FUNCTION zzr_nosuch() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  BEGIN RETURN 'x'; EXCEPTION WHEN no_such_condition THEN RETURN 'y'; END;
END $$;
-- ============================================================================
-- 5. A declaration is a type, and a type has to be one the grammar can read
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "99999999999"
-- end-expected-error
CREATE FUNCTION zzr_widelen() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v varchar(99999999999);
BEGIN v := 'x'; RETURN v; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "2147483648"
-- end-expected-error
CREATE FUNCTION zzr_intlen() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v varchar(2147483648);
BEGIN v := 'x'; RETURN v; END $$;
CREATE FUNCTION zzr_toolong() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v varchar(3);
BEGIN v := 'abcdef'; RETURN v; END $$;
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
SELECT zzr_toolong() AS a;
CREATE FUNCTION zzr_collate() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text COLLATE "C" := 'x';
BEGIN RETURN v; END $$;
-- begin-expected
-- columns: a
-- row: x
-- end-expected
SELECT zzr_collate() AS a;
CREATE FUNCTION zzr_quoted() RETURNS int LANGUAGE plpgsql AS $$
DECLARE "my var" int := 3;
BEGIN RETURN "my var"; END $$;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT zzr_quoted() AS a;
CREATE FUNCTION zzr_bpchar() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v char(5) := 'ab';
BEGIN RETURN '[' || v || ']' || length(v) || (v = 'ab'); END $$;
-- begin-expected
-- columns: a
-- row: [ab]2true
-- end-expected
SELECT zzr_bpchar() AS a;
CREATE FUNCTION zzr_bpret() RETURNS char(5) LANGUAGE plpgsql AS $$
DECLARE v char(5) := 'ab';
BEGIN RETURN v; END $$;
-- begin-expected
-- columns: a
-- row: ab   
-- end-expected
SELECT zzr_bpret() AS a;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT length(zzr_bpret()) AS a;
-- begin-expected
-- columns: a
-- row: character
-- end-expected
SELECT pg_typeof(zzr_bpret())::text AS a;
-- begin-expected
-- columns: a
-- row: [ab]
-- end-expected
SELECT '[' || zzr_bpret() || ']' AS a;
CREATE FUNCTION zzr_numscale() RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v numeric(4,2);
BEGIN v := 1.23456; RETURN v; END $$;
-- begin-expected
-- columns: a
-- row: 1.23
-- end-expected
SELECT zzr_numscale() AS a;
-- ============================================================================
-- 6. A row variable holds a row, and a scalar target holds one column
-- ============================================================================
CREATE FUNCTION zzr_rowtype() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r zzr_t%ROWTYPE;
BEGIN
  SELECT * INTO r FROM zzr_t WHERE id = 1;
  RETURN r.nm;
END $$;
-- begin-expected
-- columns: a
-- row: a
-- end-expected
SELECT zzr_rowtype() AS a;
CREATE FUNCTION zzr_rowfields() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r zzr_t%ROWTYPE;
BEGIN
  r.id := 5; r.nm := 'z';
  RETURN r.id || '/' || r.nm;
END $$;
-- begin-expected
-- columns: a
-- row: 5/z
-- end-expected
SELECT zzr_rowfields() AS a;
CREATE FUNCTION zzr_recfields() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
  SELECT id, nm INTO r FROM zzr_t WHERE id = 2;
  RETURN r.id || '/' || r.nm;
END $$;
-- begin-expected
-- columns: a
-- row: 2/b
-- end-expected
SELECT zzr_recfields() AS a;
CREATE FUNCTION zzr_unset() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r zzr_t%ROWTYPE;
BEGIN RETURN coalesce(r.nm, 'null-field'); END $$;
-- begin-expected
-- columns: a
-- row: null-field
-- end-expected
SELECT zzr_unset() AS a;
CREATE FUNCTION zzr_scalarinto() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text;
BEGIN
  SELECT id, nm INTO v FROM zzr_t WHERE id = 1;
  RETURN v;
END $$;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT zzr_scalarinto() AS a;
CREATE FUNCTION zzr_twointo() RETURNS text LANGUAGE plpgsql AS $$
DECLARE a int; b text;
BEGIN
  SELECT id, nm INTO a, b FROM zzr_t WHERE id = 2;
  RETURN a || '/' || b;
END $$;
-- begin-expected
-- columns: a
-- row: 2/b
-- end-expected
SELECT zzr_twointo() AS a;
CREATE FUNCTION zzr_intoword() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text;
BEGIN
  SELECT ' INTO ' INTO v;
  RETURN v;
END $$;
-- begin-expected
-- columns: a
-- row:  INTO 
-- end-expected
SELECT zzr_intoword() AS a;
CREATE FUNCTION zzr_strictmany() RETURNS text LANGUAGE plpgsql AS $$
DECLARE a int;
BEGIN
  SELECT id INTO STRICT a FROM zzr_t;
  RETURN a::text;
EXCEPTION WHEN too_many_rows THEN RETURN 'too many';
END $$;
-- begin-expected
-- columns: a
-- row: too many
-- end-expected
SELECT zzr_strictmany() AS a;
-- ============================================================================
-- 7. What comes out is the shape and the type the declaration promised
-- ============================================================================
CREATE FUNCTION zzr_composite() RETURNS zzr_t LANGUAGE plpgsql AS $$
DECLARE r zzr_t%ROWTYPE;
BEGIN
  SELECT * INTO r FROM zzr_t WHERE id = 1;
  RETURN r;
END $$;
-- begin-expected
-- columns: id | nm
-- row: 1 | a
-- end-expected
SELECT * FROM zzr_composite();
-- begin-expected
-- columns: a
-- row: (1,a)
-- end-expected
SELECT zzr_composite()::text AS a;
-- begin-expected
-- columns: a
-- row: zzr_t
-- end-expected
SELECT pg_typeof(zzr_composite())::text AS a;
CREATE FUNCTION zzr_outrec(OUT a int, OUT b text) RETURNS SETOF record LANGUAGE plpgsql AS $$
BEGIN
  a := 1; b := 'x'; RETURN NEXT;
  a := 2; b := 'y'; RETURN NEXT;
END $$;
-- begin-expected
-- columns: a | b
-- row: 1 | x
-- row: 2 | y
-- end-expected
SELECT * FROM zzr_outrec();
CREATE FUNCTION zzr_rounded() RETURNS int LANGUAGE plpgsql AS $$
BEGIN RETURN 4.7; END $$;
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT zzr_rounded() AS a;
CREATE FUNCTION zzr_readnum() RETURNS int LANGUAGE plpgsql AS $$
BEGIN RETURN '42'; END $$;
-- begin-expected
-- columns: a
-- row: 42
-- end-expected
SELECT zzr_readnum() AS a;
-- begin-expected
-- columns: a
-- row: integer
-- end-expected
SELECT pg_typeof(zzr_readnum())::text AS a;
CREATE FUNCTION zzr_wrongcol() RETURNS SETOF int LANGUAGE plpgsql AS $$
BEGIN RETURN QUERY SELECT nm FROM zzr_t; END $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM zzr_wrongcol();
CREATE FUNCTION zzr_widecol() RETURNS SETOF int LANGUAGE plpgsql AS $$
BEGIN RETURN QUERY SELECT id::bigint FROM zzr_t; END $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM zzr_widecol();
CREATE FUNCTION zzr_manycol() RETURNS SETOF int LANGUAGE plpgsql AS $$
BEGIN RETURN QUERY SELECT id, id FROM zzr_t; END $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM zzr_manycol();
CREATE FUNCTION zzr_tablecol() RETURNS TABLE(x int, y text) LANGUAGE plpgsql AS $$
BEGIN RETURN QUERY SELECT nm, nm FROM zzr_t; END $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM zzr_tablecol();
CREATE FUNCTION zzr_tableok() RETURNS TABLE(x int, y text) LANGUAGE plpgsql AS $$
BEGIN RETURN QUERY SELECT id, nm FROM zzr_t ORDER BY id; END $$;
-- begin-expected
-- columns: x | y
-- row: 1 | a
-- row: 2 | b
-- end-expected
SELECT * FROM zzr_tableok();
CREATE FUNCTION zzr_setrows() RETURNS SETOF zzr_t LANGUAGE plpgsql AS $$
BEGIN RETURN QUERY SELECT * FROM zzr_t ORDER BY id; END $$;
-- begin-expected
-- columns: id | nm
-- row: 1 | a
-- row: 2 | b
-- end-expected
SELECT * FROM zzr_setrows();
-- ============================================================================
-- 8. A cursor may be opened on a query the body works out
-- ============================================================================
CREATE FUNCTION zzr_openexec() RETURNS int LANGUAGE plpgsql AS $$
DECLARE c refcursor; n int;
BEGIN
  OPEN c FOR EXECUTE 'SELECT count(*) FROM zzr_t';
  FETCH c INTO n;
  CLOSE c;
  RETURN n;
END $$;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT zzr_openexec() AS a;
CREATE FUNCTION zzr_openusing() RETURNS int LANGUAGE plpgsql AS $$
DECLARE c refcursor; n int;
BEGIN
  OPEN c FOR EXECUTE 'SELECT count(*) FROM zzr_t WHERE id > $1' USING 1;
  FETCH c INTO n;
  CLOSE c;
  RETURN n;
END $$;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT zzr_openusing() AS a;
-- ============================================================================
-- 9. A routine that does not exist is named by the signature it was asked for
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzr_nosuchname() does not exist
-- end-expected-error
DROP FUNCTION zzr_nosuchname();
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzr_nosuchname(integer) does not exist
-- end-expected-error
DROP FUNCTION zzr_nosuchname(int);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzr_nosuchname(text, numeric) does not exist
-- end-expected-error
DROP FUNCTION zzr_nosuchname(text, numeric);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not find a function named "zzr_nosuchname"
-- end-expected-error
DROP FUNCTION zzr_nosuchname;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zzr_nosuchproc() does not exist
-- end-expected-error
DROP PROCEDURE zzr_nosuchproc();
DROP TABLE zzr_t CASCADE;
