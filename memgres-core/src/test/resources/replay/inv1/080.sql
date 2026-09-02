-- source: investigation.md
-- finding: 80
-- title: `GROUP BY` validation is almost entirely absent ⚠️ high
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT b FROM t GROUP BY a + 0;
-- PG: 42803 b must appear in GROUP BY | mg: returns b
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT b FROM t GROUP BY a::text;
-- PG: 42803 | mg: returns b
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT b FROM t GROUP BY abs(a);
-- PG: 42803 | mg: returns b
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT b FROM t GROUP BY lower('x');
-- PG: 42803 | mg: returns b
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT count(*) FROM t GROUP BY 3;
-- only 1 output column; PG: 42P10 position 3 not in select list
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT count(*) FROM t GROUP BY 0;
-- PG: 42P10 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT count(*) FROM t GROUP BY 'x';
-- PG: 42601 non-integer constant in GROUP BY | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT count(*) FROM t GROUP BY NULL;
-- PG: 42601 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT b AS a, count(*) FROM t GROUP BY a;
-- "a" is t.a, so b is ungrouped
--   PG: 42803 column "t.b" must appear in the GROUP BY clause | mg: returns b
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT count(*) FROM t HAVING a > 0;
-- PG: 42803 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT count(*) FROM t HAVING sum(a) > 0 AND b > 0;
-- PG: 42803 | mg: OK;
