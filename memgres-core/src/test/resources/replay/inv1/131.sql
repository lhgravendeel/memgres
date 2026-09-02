-- source: investigation.md
-- finding: 131
-- title: A function's returned record type is not checked
-- f declared RETURNS pl_two_int8s but its RETURN yields a differently-shaped row
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pl_retc(integer) does not exist
-- end-expected-error
SELECT (pl_retc(42)).q1;
--   PG: 42804 returned record type does not match expected record type | mg: returns 42;
