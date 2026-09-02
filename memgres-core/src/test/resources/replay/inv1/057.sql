-- source: investigation.md
-- finding: 57
-- title: Array literal parsing accepts malformed input ⚠️ (12 cases)
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{"a"b}"
-- end-expected-error
SELECT '{"a"b}'::text[];
-- mg: {a,b}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{a"b"}"
-- end-expected-error
SELECT '{a"b"}'::text[];
-- mg: {"a\"b\""}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{"a""b"}"
-- end-expected-error
SELECT '{"a""b"}'::text[];
-- mg: {a,b}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{{1,{2}},{2,3}}"
-- end-expected-error
SELECT '{{1,{2}},{2,3}}'::text[];
-- mg: accepted verbatim
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{}}"
-- end-expected-error
SELECT '{}}'::text[];
-- mg: {"}"}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{ }}"
-- end-expected-error
SELECT '{ }}'::text[];
-- mg: {"}"}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "}{"
-- end-expected-error
SELECT '}{'::text[];
-- mg: }{
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{foo{}}"
-- end-expected-error
SELECT '{foo{}}'::text[];
-- mg: {"foo{}"}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{foo,,bar}"
-- end-expected-error
SELECT '{foo,,bar}'::text[];
-- mg: {foo,"",bar}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{1,}"
-- end-expected-error
SELECT '{1,}'::text[];
-- mg: {1}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{{1,}}"
-- end-expected-error
SELECT '{{1,}}'::text[];
-- mg: {{1}}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{{"1 2" x},{3}}"
-- end-expected-error
SELECT '{{"1 2" x},{3}}'::text[];
-- mg: {{"1 2",x},{3}};
