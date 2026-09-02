-- source: investigation.md
-- finding: 126
-- title: `%TYPE` and `%ROWTYPE` resolve against objects that do not exist ⚠️
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
DO $$ declare x pl_nosuch%type;            begin null; end $$  -- PG: 42704 variable does not exist | mg: OK
DO $$ declare x pl_nosuch.bar%type;        begin null; end $$  -- PG: 42P01 relation does not exist | mg: OK
DO $$ declare x public.pl_misc_table.zed%type; begin null; end $$
--   PG: 42703 column "zed" of relation "pl_misc_table" does not exist | mg: OK
DO $$ declare x pl_nosuch%rowtype;         begin null; end $$  -- PG: 42P01 | mg: OK
DO $$ declare x pl_nosuch.bar%rowtype;     begin null; end $$  -- PG: 3F000 schema does not exist | mg: OK;
