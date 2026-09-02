-- source: investigation.md
-- finding: 83
-- title: `uuid` and `bit` input parsers are wrong in both directions
-- begin-expected
-- columns: uuid:uuid
-- row: a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
-- rowcount: 1
-- end-expected
SELECT 'a0eebc999c0b4ef8bb6d6bb9bd380a11'::uuid;
-- undashed; PG: works | mg: 22P02
-- begin-expected
-- columns: uuid:uuid
-- row: a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
-- rowcount: 1
-- end-expected
SELECT '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'::uuid;
-- braced;   PG: works | mg: 22P02
-- begin-expected
-- columns: uuid:uuid
-- row: a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
-- rowcount: 1
-- end-expected
SELECT 'a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11'::uuid;
-- regrouped;PG: works | mg: 22P02
-- begin-expected
-- columns: varbit:varbit
-- row: 101
-- rowcount: 1
-- end-expected
SELECT 'b101'::varbit;
-- 'b' radix prefix; PG: 101      | mg: 22P02 not a valid binary digit
-- begin-expected
-- columns: varbit:varbit
-- row: 00011111
-- rowcount: 1
-- end-expected
SELECT 'x1f'::varbit;
-- 'x' hex prefix;   PG: 00011111 | mg: 22P02
-- begin-expected
-- columns: varbit:varbit
-- row: 00011111
-- rowcount: 1
-- end-expected
SELECT 'X1F'::varbit;
-- PG: 00011111 | mg: 22P02
-- begin-expected
-- columns: bit:bit
-- row: 101
-- rowcount: 1
-- end-expected
SELECT 'B101'::bit(3);
-- PG: 101      | mg: 22P02
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type uuid: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1"
-- end-expected-error
SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1'::uuid;
-- one digit short
--   PG: 22P02 | mg: a0eebc99-9c0b-4ef8-bb6d-06bb9bd380a1   ← a zero inserted mid-value
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type uuid: "1-1-1-1-1"
-- end-expected-error
SELECT '1-1-1-1-1'::uuid;
--   PG: 22P02 | mg: 00000001-0001-0001-0001-000000000001;
