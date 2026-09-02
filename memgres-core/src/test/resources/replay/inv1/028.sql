-- source: investigation.md
-- finding: 28
-- title: `FOR UPDATE` legality is not checked
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT
-- end-expected-error
SELECT i FROM t UNION SELECT 1 FOR UPDATE;
--   PG: 0A000 FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT DISTINCT i FROM t FOR UPDATE;
--   PG: 0A000 FOR UPDATE is not allowed with DISTINCT clause        | mg: OK;
