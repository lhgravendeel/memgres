-- source: investigation.md
-- finding: 11
-- title: PL/pgSQL declaration semantics
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CONSTANT"
-- end-expected-error
DECLARE c CONSTANT int := 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "c"
-- end-expected-error
BEGIN c := 2;
-- PG: 22005 variable is declared CONSTANT | mg: 2
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "int"
-- end-expected-error
DECLARE x int NOT NULL := 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "x"
-- end-expected-error
BEGIN x := NULL;
-- PG: 22004 | mg: NULL assigned
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "int"
-- end-expected-error
DECLARE x int NOT NULL;
-- PG: 22004 must have a default, since declared NOT NULL | mg: accepted
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "varchar"
-- end-expected-error
DECLARE v varchar(3);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "v"
-- end-expected-error
BEGIN v := 'abcdef';
-- PG: 22001 value too long | mg: 'abcdef';
