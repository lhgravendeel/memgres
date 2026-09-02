-- source: review-2026-08.md
-- finding: Root cause 10: user-operator dispatch treats every numeric type as the same type
-- area: SQL-level cursors
-- title: Root cause 10: user-operator dispatch treats every numeric type as the same type
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_udadd(int, int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT $1 * 100 + $2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR ###@ (LEFTARG = int, RIGHTARG = int, FUNCTION = zz_udadd);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: numeric ###@ numeric
-- end-expected-error
SELECT 1.5 ###@ 2.5;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: bigint ###@ bigint
-- end-expected-error
SELECT 1::bigint ###@ 2::bigint;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT 'a' ###@ 'b';
