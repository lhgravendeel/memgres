-- source: investigation.md
-- finding: 31
-- title: Outer joins lose the left alias ⚠️ high — rejects valid SQL
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t1" does not exist
-- end-expected-error
SELECT a.i, b.j FROM t1 a FULL JOIN t2 b ON a.i = b.j ORDER BY b.j;
--   PG: rows with NULL on the unmatched side
--   mg: 42P01 missing FROM-clause entry for table "a"

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t1" does not exist
-- end-expected-error
SELECT (a.i IS NULL)::text, b.j FROM t1 a RIGHT JOIN t2 b ON false ORDER BY b.j;
--   PG: two rows | mg: 42P01 missing FROM-clause entry for table "a";
