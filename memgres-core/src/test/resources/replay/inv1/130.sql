-- source: investigation.md
-- finding: 130
-- title: Composite field access: unchecked writes, unparsed reads ⚠️ both directions
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
DO $$ declare c pl_two_int8s; begin c.x = 1; end $$
--   PG: 42703 record "c" has no field "x" | mg: OK
DO $$ declare c pl_nested_int8s; begin c.c2.x = 1; end $$
--   PG: 42703 cannot assign to field "x" … no such column in data type | mg: OK
DO $$ declare r1 pl_two_int8s; v text; begin v := r1.q1::text; end $$
--   PG: works | mg: 42601 syntax error at or near ".";
