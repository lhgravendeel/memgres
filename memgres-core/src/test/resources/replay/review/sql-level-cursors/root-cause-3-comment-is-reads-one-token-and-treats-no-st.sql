-- source: review-2026-08.md
-- finding: Root cause 3: COMMENT ... IS reads one token and treats "no string literal" as "remove the comment"
-- area: SQL-level cursors
-- title: Root cause 3: COMMENT ... IS reads one token and treats "no string literal" as "remove the comment"
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_sp" does not exist
-- end-expected-error
COMMENT ON TABLE zz_sp IS 'keep me';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "||"
-- end-expected-error
COMMENT ON TABLE zz_sp IS 'a' || 'b';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_sp" does not exist
-- end-expected-error
SELECT obj_description('zz_sp'::regclass,'pg_class');
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "42"
-- end-expected-error
COMMENT ON TABLE zz_sp IS 42;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "current_user"
-- end-expected-error
COMMENT ON TABLE zz_sp IS current_user;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_sp" does not exist
-- end-expected-error
SELECT obj_description('zz_sp'::regclass,'pg_class');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_sp" does not exist
-- end-expected-error
COMMENT ON TABLE zz_sp IS '';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_sp" does not exist
-- end-expected-error
SELECT obj_description('zz_sp'::regclass,'pg_class') IS NULL AS removed;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_sp" does not exist
-- end-expected-error
SELECT count(*) FROM pg_description WHERE objoid = 'zz_sp'::regclass;
