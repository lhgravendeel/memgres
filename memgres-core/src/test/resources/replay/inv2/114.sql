-- source: investigation-2026-08.md
-- finding: 114
-- title: Expressions are evaluated value-by-value with no static type resolution, so CASE branches are never unified, a simple CASE operand is never checked against its 
-- begin-expected-error
-- sqlstate: 42846
-- message-like: CASE/WHEN could not convert type integer[] to text[]
-- end-expected-error
SELECT CASE WHEN false THEN ARRAY[1] ELSE ARRAY['a'] END;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT CASE 1 WHEN 'a' THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text = integer
-- end-expected-error
SELECT CASE 'a' WHEN 1 THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT CASE WHEN 1=1 THEN 'a' ELSE (SELECT 1/0) END;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - text
-- end-expected-error
SELECT -('abc'::text);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - boolean
-- end-expected-error
SELECT -(true);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - date
-- end-expected-error
SELECT -(date '2020-01-01');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - uuid
-- end-expected-error
SELECT -('11111111-1111-1111-1111-111111111111'::uuid);
