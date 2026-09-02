-- source: investigation-2026-08.md
-- finding: 306
-- title: The COMMENT grammar collects tokens until IS, discards everything from the first '(' on, and skips anything after IS that is not a string literal or NULL; the e
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer #%^&* integer
-- end-expected-error
COMMENT ON OPERATOR #%^&* (int, int) IS 'nope';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "zz_nosuchcoll" for encoding "UTF8" does not exist
-- end-expected-error
COMMENT ON COLLATION zz_nosuchcoll IS 'nope';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: aggregate zz_nosuchagg(integer) does not exist
-- end-expected-error
COMMENT ON AGGREGATE zz_nosuchagg(int) IS 'nope';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
COMMENT ON TABLE zz_t IS 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "true"
-- end-expected-error
COMMENT ON TABLE zz_t IS true;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DEFAULT"
-- end-expected-error
COMMENT ON TABLE zz_t IS DEFAULT;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "||"
-- end-expected-error
COMMENT ON TABLE zz_t IS 'a' || 'b';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'y'"
-- end-expected-error
COMMENT ON TABLE zz_t IS 'x' 'y';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "N"
-- end-expected-error
COMMENT ON TABLE zz_t IS N'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
COMMENT ON TABLE zz_t, zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: column name must be qualified
-- end-expected-error
COMMENT ON COLUMN zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "zz_t"
-- end-expected-error
COMMENT ON zz_t IS 'x';
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '7MB';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "work_mem": "DEFAULT"
-- end-expected-error
SET work_mem = 'DEFAULT';
-- begin-expected
-- columns: work_mem:text
-- row: 7MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
