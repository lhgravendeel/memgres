-- source: investigation.md
-- finding: 104
-- title: 581 functions have no return type; three have inconsistent arity
-- begin-expected
-- columns: proname:name
-- rowcount: 0
-- end-expected
SELECT proname FROM pg_proc WHERE prorettype = 0;
-- PG: 0   memgres: 581
-- begin-expected
-- columns: proname:name
-- rowcount: 0
-- end-expected
SELECT proname FROM pg_proc WHERE prokind = 'f'
   AND pronargs <> coalesce(array_length(string_to_array(trim(proargtypes::text),' '),1),0);
--   PG: 0   memgres: 3   (pg_sleep, pg_sleep_for, pg_sleep_until);
