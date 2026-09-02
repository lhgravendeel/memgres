-- source: review-2026-08.md
-- finding: Root cause 5: SECURITY LABEL's parser reads the provider and discards the rest of the statement
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 5: SECURITY LABEL's parser reads the provider and discards the rest of the statement
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "zz_t"
-- end-expected-error
SECURITY LABEL ON zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "IS"
-- end-expected-error
SECURITY LABEL ON TABLE IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SECURITY LABEL ON TABLE zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SECURITY LABEL ON TABLE zz_t IS;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
SECURITY LABEL ON TABLE zz_t IS 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ON"
-- end-expected-error
SECURITY LABEL FOR ON TABLE zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
SECURITY LABEL FOR selinux, other ON TABLE zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "TRIGGER"
-- end-expected-error
SECURITY LABEL ON TRIGGER tr ON zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "RULE"
-- end-expected-error
SECURITY LABEL ON RULE r ON zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "POLICY"
-- end-expected-error
SECURITY LABEL ON POLICY p ON zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CONSTRAINT"
-- end-expected-error
SECURITY LABEL ON CONSTRAINT c ON zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "OPERATOR"
-- end-expected-error
SECURITY LABEL ON OPERATOR + (int, int) IS 'x';
