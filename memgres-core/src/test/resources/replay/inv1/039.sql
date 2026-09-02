-- source: investigation.md
-- finding: 39
-- title: LATERAL on the nullable side of an outer join
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT * FROM t RIGHT JOIN LATERAL (SELECT t.i AS z) s ON true;
--   PG: 42P10 invalid reference to FROM-clause entry for table "t" | mg: returns rows
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT * FROM t FULL JOIN LATERAL (SELECT t.i AS z) s ON true;
-- same;
