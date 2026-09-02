-- source: investigation.md
-- finding: 74
-- title: Numeric-to-integer casts wrap instead of erroring ⚠️ high — silent corruption
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT '9223372036854775808'::numeric::int8;
--   PG: 22003 bigint out of range | mg: -9223372036854775808   ← sign flipped
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT '-9223372036854775809'::numeric::int8;
--   PG: 22003 | mg: 9223372036854775807                        ← sign flipped
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT '99999999999999999999'::numeric::bigint;
--   PG: 22003 | mg: 7766279631452241919                        ← unrelated value;
