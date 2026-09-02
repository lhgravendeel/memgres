-- source: investigation-2026-08.md
-- finding: 307
-- title: SECURITY LABEL's parser reads the optional provider and advances to the semicolon, so the provider check is the only thing standing between any token sequence a
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
