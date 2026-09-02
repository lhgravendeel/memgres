-- source: review-2026-08.md
-- finding: Operand-mismatch messages are assembled from literals
-- area: Aggregates, window functions and grouping
-- title: Operand-mismatch messages are assembled from literals
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT 1 IN ('x');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric: "x"
-- end-expected-error
SELECT 1.5 IN ('x');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT 1::bigint IN ('x');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = text
-- end-expected-error
SELECT 1 = ANY (SELECT 'x');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: numeric > text
-- end-expected-error
SELECT 1.5 > ANY (SELECT 'x');
