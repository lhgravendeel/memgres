-- source: investigation.md
-- finding: 87
-- title: Type-modifier and value-size caps absent (3 cases)
-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type bit cannot exceed 83886080
-- end-expected-error
SELECT '0'::bit(200000000);
--   PG: 54000 length for type bit cannot exceed 83886080 | mg: allocates 200 000 000 bits
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT factorial(50000);
--   PG: value overflows numeric format | mg: computes a 213 237-digit number
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT '1e200000'::numeric + 1;
--   PG: value overflows numeric format | mg: computes a 200 001-digit number;
