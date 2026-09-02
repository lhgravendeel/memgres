-- source: investigation.md
-- finding: 124
-- title: Domain constraints are not applied to PL/pgSQL variables ⚠️ high
-- domain pl_int_nn AS int NOT NULL
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
DO $$ declare x pl_int_nn; begin null; end$$              -- PG: 23502 | mg: OK
DO $$ declare x pl_int_nn := 42; begin x := null; end$$   -- PG: 23502 | mg: OK

-- domain pl_pos AS int CHECK (VALUE > 0)
DO $$ declare x pl_pos := -1; begin null; end$$           -- PG: 23514 | mg: OK
DO $$ declare x pl_pos := 5; begin x := -3; end$$         -- PG: 23514 | mg: OK
DO $$ declare x pl_pos := 5; begin select -7 into x; end$$-- PG: 23514 | mg: OK;
