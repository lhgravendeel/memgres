-- source: investigation.md
-- finding: 85
-- title: Range bounds truncate silently to a different range ⚠️ high — silent corruption
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value "99999999999999999999999" is out of range for type integer
-- end-expected-error
SELECT '[1,99999999999999999999999)'::int4range;
--   PG: 22003 value "99999999999999999999999" is out of range for type integer
--   mg: [1,200376420520689663)
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "3.5"
-- end-expected-error
SELECT '[1,3.5)'::int4range;
-- PG: 22P02 invalid input syntax for type integer: "3.5" | mg: [1,3.5);
