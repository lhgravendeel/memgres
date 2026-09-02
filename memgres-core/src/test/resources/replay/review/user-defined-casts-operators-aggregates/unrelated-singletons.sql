-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Unrelated singletons
-- begin-expected
-- columns: ?column?:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT 1 OPERATOR(+) 2;
-- begin-expected
-- columns: ?column?:text
-- row: ab
-- rowcount: 1
-- end-expected
SELECT 'a' OPERATOR(||) 'b';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT true OPERATOR(=) true;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_eq(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###@ (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_eq);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT a FROM zz_t WHERE a ###@ ANY (ARRAY[21, 99]) ORDER BY a;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT a FROM zz_t WHERE a ###@ ALL (ARRAY[21, 31]) ORDER BY a;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT a FROM zz_t WHERE a ###@ ANY (SELECT b FROM zz_t) ORDER BY a;
-- begin-expected
-- columns: pg_describe_object:text
-- rowcount: 0
-- end-expected
SELECT pg_describe_object('pg_operator'::regclass, oid, 0) FROM pg_operator WHERE oprname = '###@';
-- begin-expected
-- columns: pg_collation_actual_version:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_collation_actual_version((SELECT oid FROM pg_collation WHERE collname='C' LIMIT 1));
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a>=b": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[{"a>=b":1}]', '$[*] ? (@."a>=b" == 1)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a<=b": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[{"a<=b":1}]', '$[*] ? (@."a<=b" == 1)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a==b": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[{"a==b":1}]', '$[*] ? (@."a==b" == 1)');
-- begin-expected
-- columns: jsonb_path_query:jsonb
-- row: {"a!=b": 1}
-- rowcount: 1
-- end-expected
SELECT jsonb_path_query('[{"a!=b":1}]', '$[*] ? (@."a!=b" == 1)');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: @@ path
-- end-expected-error
SELECT @@ '[(0,0),(2,0)]'::path;
