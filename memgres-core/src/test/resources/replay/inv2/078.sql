-- source: investigation-2026-08.md
-- finding: 78
-- title: TsQuery.tokenize has a path that consumes no character: a '<' with no later '>' falls through every branch, and the bare-word loop excludes '<', so the outer wh
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
