-- Residual type-facade bugs from bugs-review.md (follow-up to PRs #79-#81).
-- Expected values verified against real PostgreSQL 18.
-- Covers: H18, H20, H21, H22, H23, H26, M23, M24, M1, L1, L3.

-- ============================================================
-- H18: abbreviated cidr input; single-octet is cidr-only (inet rejects it)
-- ============================================================

-- begin-expected
-- columns: result
-- row: 10.0.0.0/8
-- end-expected
SELECT '10'::cidr::text AS result;

-- begin-expected
-- columns: result
-- row: 10.1.0.0/16
-- end-expected
SELECT '10.1'::cidr::text AS result;

-- begin-expected
-- columns: result
-- row: 128.1.0.0/16
-- end-expected
SELECT '128.1'::cidr::text AS result;

-- begin-expected
-- columns: result
-- row: 192.168.1.0/24
-- end-expected
SELECT '192.168.1'::cidr::text AS result;

-- begin-expected
-- columns: result
-- row: 10.0.0.0/8
-- end-expected
SELECT '10/8'::cidr::text AS result;

-- begin-expected-error
-- message-like: invalid input syntax for type inet
-- end-expected-error
SELECT '10'::inet;

-- begin-expected-error
-- message-like: invalid input syntax for type inet
-- end-expected-error
SELECT '10.1'::inet;

-- ============================================================
-- H20: abbrev() on IPv6 cidr; inet_merge cross-family errors 22023
-- ============================================================

-- begin-expected
-- columns: result
-- row: 2001:db8/32
-- end-expected
SELECT abbrev('2001:db8::/32'::cidr) AS result;

-- begin-expected
-- columns: result
-- row: 2001:db8::1/64
-- end-expected
SELECT abbrev('2001:db8:0:1::/64'::cidr) AS result;

-- begin-expected
-- columns: result
-- row: 2001:db8::ff00/104
-- end-expected
SELECT abbrev('2001:db8::ff00:0/104'::cidr) AS result;

-- begin-expected-error
-- message-like: cannot merge addresses from different families
-- end-expected-error
SELECT inet_merge('10.0.0.0/8'::inet, '2001:db8::/32'::inet);

-- ============================================================
-- H21: macaddr8 -> macaddr only when the middle bytes are ff:fe
-- ============================================================

-- begin-expected
-- columns: result
-- row: 01:02:03:04:05:06
-- end-expected
SELECT ('01:02:03:ff:fe:04:05:06'::macaddr8)::macaddr::text AS result;

-- begin-expected-error
-- message-like: macaddr8 data out of range to convert to macaddr
-- end-expected-error
SELECT ('01:02:03:04:05:06:07:08'::macaddr8)::macaddr;

-- ============================================================
-- L3: text(inet) keeps the /32 suffix
-- ============================================================

-- begin-expected
-- columns: result
-- row: 1.2.3.4/32
-- end-expected
SELECT text('1.2.3.4'::inet) AS result;

-- ============================================================
-- H22: bit(n) exact-length enforcement on INSERT (explicit cast still pads)
-- ============================================================

CREATE TABLE zz_facade_bit (b bit(3));

-- begin-expected-error
-- message-like: does not match type bit(3)
-- end-expected-error
INSERT INTO zz_facade_bit VALUES (B'10');

INSERT INTO zz_facade_bit VALUES (B'100');
INSERT INTO zz_facade_bit VALUES (B'10'::bit(3));

-- begin-expected
-- columns: b
-- row: 100
-- row: 100
-- end-expected
SELECT b::text AS b FROM zz_facade_bit ORDER BY b;

DROP TABLE zz_facade_bit;

-- ============================================================
-- H23: position(bytea in bytea); set_byte returns bytea
-- ============================================================

-- begin-expected
-- columns: result
-- row: 2
-- end-expected
SELECT position('\x34'::bytea in '\x123456'::bytea) AS result;

-- begin-expected
-- columns: result
-- row: \xff34
-- end-expected
SELECT set_byte('\x1234'::bytea, 0, 255)::text AS result;

-- ============================================================
-- H26: regexp_replace backref to missing group; SQL-regex substring
-- ============================================================

-- begin-expected
-- columns: result
-- row: ac
-- end-expected
SELECT regexp_replace('abc', 'b', '\1') AS result;

-- begin-expected
-- columns: result
-- row: oob
-- end-expected
SELECT substring('foobar' from '%#"o_b#"%' for '#') AS result;

-- ============================================================
-- M23: concat_ws with only the separator argument errors
-- ============================================================

-- begin-expected-error
-- message-like: function concat_ws(unknown) does not exist
-- end-expected-error
SELECT concat_ws(',');

-- ============================================================
-- M1: jsonb scalar -> 0 echoes the scalar; other indexes NULL
-- ============================================================

-- begin-expected
-- columns: result
-- row: 123
-- end-expected
SELECT '123'::jsonb -> 0 AS result;

-- begin-expected
-- columns: result
-- row: "abc"
-- end-expected
SELECT '"abc"'::jsonb -> 0 AS result;

-- begin-expected
-- columns: result
-- row:
-- end-expected
SELECT '123'::jsonb -> 1 AS result;

-- ============================================================
-- L1: negative substring length on bytea
-- ============================================================

-- begin-expected-error
-- message-like: negative substring length not allowed
-- end-expected-error
SELECT substring('\x123456'::bytea from 1 for -1);
