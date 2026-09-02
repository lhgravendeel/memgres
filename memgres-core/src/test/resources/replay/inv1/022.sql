-- source: investigation.md
-- finding: 22
-- title: Function overload resolution for unknown literals
-- two candidates: fn_num(int) and fn_num(double precision)
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function fn_num(unknown) does not exist
-- end-expected-error
SELECT fn_num('6');
-- PG: resolves to double precision | mg: 42883 fn_num(text) does not exist;
