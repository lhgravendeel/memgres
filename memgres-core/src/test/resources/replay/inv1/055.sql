-- source: investigation.md
-- finding: 55
-- title: `REFRESH MATERIALIZED VIEW CONCURRENTLY` prerequisites
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "m" does not exist
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY m;
-- m has no unique index
--   PG: 55000 cannot refresh materialized view "public.m" concurrently | mg: accepted
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "m" does not exist
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY m WITH NO DATA;
--   PG: 42601 CONCURRENTLY and WITH NO DATA cannot be used together    | mg: accepted;
