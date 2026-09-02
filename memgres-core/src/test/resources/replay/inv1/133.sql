-- source: investigation.md
-- finding: 133
-- title: Cursor parameter binding is unvalidated, and mixed notation is rejected ⚠️ both directions
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "open"
-- end-expected-error
open c1(param2 := 20, 21);
-- PG: 42601 value for parameter "param2" specified more than once | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "open"
-- end-expected-error
open c1(20, param1 := 21);
-- PG: 42601 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "open"
-- end-expected-error
open c1 (p2 := 77, p2 := 42);
-- PG: 42601 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "open"
-- end-expected-error
open c1 (p2 := 77);
-- PG: 42601 not enough arguments for cursor "c1" | mg: OK
-- and a correct named-parameter open:
--   PG: works | mg: 42601 "param2" is not a known variable;
