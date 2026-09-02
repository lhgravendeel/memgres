-- source: review-2026-08.md
-- finding: Root cause 4: TsQuery.tokenize has a path that consumes no character
-- area: String, date/time, geometric, range and interval implementations
-- title: Root cause 4: TsQuery.tokenize has a path that consumes no character
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a<b"
-- end-expected-error
SELECT 'a<b'::tsquery;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "cat <2 dog"
-- end-expected-error
SELECT to_tsquery('cat <2 dog');
