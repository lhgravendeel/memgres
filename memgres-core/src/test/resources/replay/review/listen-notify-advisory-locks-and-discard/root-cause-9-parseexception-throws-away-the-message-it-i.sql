-- source: review-2026-08.md
-- finding: Root cause 9: ParseException throws away the message it is constructed with
-- area: LISTEN/NOTIFY, advisory locks and DISCARD
-- title: Root cause 9: ParseException throws away the message it is constructed with
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized VACUUM option "bogus_option"
-- end-expected-error
VACUUM (BOGUS_OPTION);
-- begin-expected-error
-- sqlstate: 42P18
-- message-like: cannot determine type of empty array
-- end-expected-error
SELECT ARRAY[];
-- begin-expected-error
-- sqlstate: 42601
-- message-like: zero-length delimited identifier at or near """"
-- end-expected-error
LISTEN "";
-- begin-expected-error
-- sqlstate: 42601
-- message-like: zero-length delimited identifier at or near """"
-- end-expected-error
NOTIFY "";
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'nokeyword'"
-- end-expected-error
NOTIFY zz_ex 'nokeyword';
