-- source: investigation.md
-- finding: 125
-- title: Declaration initialisers are not type-checked
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
DO $$ declare x int := 'abc'; begin null; end$$                 -- PG: 22P02 | mg: OK
DO $$ declare x pl_misc_table.f1%type := 'abc'; begin null; end$$-- PG: 22P02 | mg: OK;
