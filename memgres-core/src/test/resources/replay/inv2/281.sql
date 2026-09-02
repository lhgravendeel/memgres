-- source: investigation-2026-08.md
-- finding: 281
-- title: Describe reads the number after '$' straight out of the SQL text with no bound and outside any error boundary: countParameters does an unguarded Integer.parseIn
-- Parse + Describe 'S'
-- begin-expected-error
-- sqlstate: 42601
-- message-like: parameter number too large at or near "$99999999999"
-- end-expected-error
SELECT $99999999999;
