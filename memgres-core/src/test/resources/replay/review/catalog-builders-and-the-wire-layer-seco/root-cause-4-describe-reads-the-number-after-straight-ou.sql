-- source: review-2026-08.md
-- finding: Root cause 4: Describe reads the number after `$` straight out of the SQL text, with no bound and outside any error boundary
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 4: Describe reads the number after `$` straight out of the SQL text, with no bound and outside any error boundary
-- Parse + Describe 'S'
-- begin-expected-error
-- sqlstate: 42601
-- message-like: parameter number too large at or near "$99999999999"
-- end-expected-error
SELECT $99999999999;
-- Parse + Describe 'S'
-- begin-expected-error
-- sqlstate: 42P02
-- message-like: there is no parameter $2000000000
-- end-expected-error
SELECT $2000000000;
-- Parse + Describe 'S'
-- begin-expected-error
-- sqlstate: 42P02
-- message-like: there is no parameter $70000
-- end-expected-error
SELECT $70000;
