-- source: review-2026-08.md
-- finding: Root cause 11: the bytea index surface computes the byte offset before it checks the index
-- area: String, date/time, geometric, range and interval implementations
-- title: Root cause 11: the bytea index surface computes the byte offset before it checks the index
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: index -1 out of valid range, 0..7
-- end-expected-error
SELECT get_bit('\xff'::bytea, -1);
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: index -1 out of valid range, 0..15
-- end-expected-error
SELECT get_bit('\x00ff'::bytea, -1);
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: index -1 out of valid range, 0..7
-- end-expected-error
SELECT set_bit('\xff'::bytea, -1, 0);
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: index 4294967296 out of valid range, 0..7
-- end-expected-error
SELECT get_bit('\xff'::bytea, 4294967296);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function get_byte(bytea, bigint) does not exist
-- end-expected-error
SELECT get_byte('\x0102'::bytea, 4294967296);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function set_byte(bytea, bigint, integer) does not exist
-- end-expected-error
SELECT set_byte('\x0102'::bytea, 4294967296, 9);
