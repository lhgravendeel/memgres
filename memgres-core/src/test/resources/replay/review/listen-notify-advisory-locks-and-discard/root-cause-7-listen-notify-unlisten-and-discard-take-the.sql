-- source: review-2026-08.md
-- finding: Root cause 7: LISTEN/NOTIFY/UNLISTEN and DISCARD take their argument from readIdentifier(), and nothing checks what is left over
-- area: LISTEN/NOTIFY, advisory locks and DISCARD
-- title: Root cause 7: LISTEN/NOTIFY/UNLISTEN and DISCARD take their argument from readIdentifier(), and nothing checks what is left over
-- begin-expected
-- ok: 0
-- end-expected
NOTIFY zz_dq, $$hi$$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
LISTEN zz_l1, zz_l2;
-- begin-expected
-- columns: string_agg:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT string_agg(c, ',') FROM pg_listening_channels() c;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'zz_lit'"
-- end-expected-error
LISTEN 'zz_lit';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'zz_lit'"
-- end-expected-error
NOTIFY 'zz_lit';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'zz_lit'"
-- end-expected-error
UNLISTEN 'zz_lit';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "select"
-- end-expected-error
LISTEN select;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "all"
-- end-expected-error
NOTIFY all;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "table"
-- end-expected-error
UNLISTEN table;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "||"
-- end-expected-error
NOTIFY zz_ex, 'a' || 'b';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
NOTIFY zz_ex, 'a', 'b';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "BOGUS"
-- end-expected-error
DISCARD BOGUS;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SEQUENCE"
-- end-expected-error
DISCARD SEQUENCE;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXTRA"
-- end-expected-error
DISCARD ALL EXTRA;
