-- source: investigation.md
-- finding: 122
-- title: `CONSTANT` is not enforced ⚠️ high
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
DO $$ declare x constant int := 1; begin select 2 into x; end$$          -- PG: 22005 | mg: OK
DO $$ declare x constant pl_var_record; begin x.f1 := 42; end$$          -- PG: 22005 | mg: OK
DO $$ declare x constant int; y int; begin for x, y in select 1,2 loop end loop; end$$
--   PG: 22005 variable "x" is declared CONSTANT | mg: OK;
