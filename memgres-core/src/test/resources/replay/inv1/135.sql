-- source: investigation.md
-- finding: 135
-- title: `RAISE` option validation absent; expression option values rejected ⚠️ both directions
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "raise"
-- end-expected-error
raise notice 'too few: %, %, %', 1, 1;
-- PG: 42601 too few parameters specified for RAISE | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "raise"
-- end-expected-error
raise notice 'trailing percent %';
-- PG: 42601 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "raise"
-- end-expected-error
raise notice 'x' using detail = 'd', detail = 'e';
--   PG: 42601 RAISE option already specified: DETAIL | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "raise"
-- end-expected-error
raise division_by_zero using message = 'custom' || ' message';
--   PG: works | mg: 42601 Expected identifier at position 55, found: ||;
