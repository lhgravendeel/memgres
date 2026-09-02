-- source: investigation.md
-- finding: 123
-- title: `NOT NULL` on a variable is not enforced ⚠️
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
DO $$ declare x record not null; begin x := row(1); end$$
--   PG: 22004 variable "x" must have a default value, since it's declared NOT NULL | mg: OK
DO $$ declare x record not null := row(42); begin x := null; end$$       -- PG: 22004 | mg: OK
DO $$ declare x pl_nn_record not null := null; begin null; end$$         -- PG: 22004 | mg: OK;
