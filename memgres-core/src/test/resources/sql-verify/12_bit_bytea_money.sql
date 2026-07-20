-- ============================================================
-- 12: bit/varbit, bytea, money type fixes
-- Tests for H22, H23, H24, L1, L2, L6
-- ============================================================

-- === H22: bit/varbit ===

-- Bit string literal
SELECT B'1010'::bit(4);
-- expect: 1010

-- Bit shift left (binary, not decimal)
SELECT B'1010' << 1;
-- expect: 0100

-- Bit shift right
SELECT B'1010' >> 1;
-- expect: 0101

-- Integer to bit cast (two's complement)
SELECT 5::bit(4);
-- expect: 0101

SELECT (-1)::bit(8);
-- expect: 11111111

-- bit(n) length enforcement: truncation
SELECT B'10101'::bit(3);
-- expect: 101

-- bit(n) length enforcement: padding
SELECT B'10'::bit(5);
-- expect: 10000

-- varbit(n) max length enforcement
-- SELECT B'10101'::varbit(3);
-- expect: ERROR 22001 (bit string too long)

-- Invalid bit digit rejected
-- SELECT '102'::bit(3);
-- expect: ERROR 22P02

-- octet_length of bit string
SELECT octet_length(B'1010101010101010');
-- expect: 2

-- bit_length of bit string
SELECT bit_length(B'1010');
-- expect: 4

-- bit to integer cast
SELECT B'0101'::integer;
-- expect: 5

-- === H23: bytea ===

-- Hex format parsing
SELECT '\x48656c6c6f'::bytea;
-- expect: \x48656c6c6f

-- Escape format parsing
SELECT '\000\047'::bytea;
-- expect: \x0027

-- md5 of bytea (hashes raw bytes, not text form)
SELECT md5('\x00'::bytea);
-- expect: 93b885adfe0da089cdf634904fd59f71

-- encode escape (single backslash for non-printable)
SELECT encode('\x00\x01\x41'::bytea, 'escape');
-- expect: \000\001A

-- Hex with whitespace (PG allows it)
SELECT '\xde ad'::bytea;
-- expect: \xdead

-- ltrim/rtrim/btrim on bytea (should not return Java array garbage)
SELECT ltrim('\x001234'::bytea, '\x00'::bytea);
-- expect: \x1234

-- === H24: money ===

-- Basic money literal
SELECT '$1,234.56'::money;
-- expect: $1,234.56

-- Parenthetical negation
SELECT '($123.45)'::money;
-- expect: -$123.45

-- Money addition
SELECT '$10.00'::money + '$20.00'::money;
-- expect: $30.00

-- Money / money = float8
SELECT '$100.00'::money / '$25.00'::money;
-- expect: 4.0

-- Money / numeric = money
SELECT '$100.00'::money / 4;
-- expect: $25.00

-- Invalid money input
-- SELECT 'abc'::money;
-- expect: ERROR 22P02

-- Money range check
-- SELECT '99999999999999999.99'::money;
-- expect: ERROR 22003

-- sum(money)
CREATE TABLE money_test (amount money);
INSERT INTO money_test VALUES ('$10.00'), ('$20.00'), ('$30.00');
SELECT sum(amount) FROM money_test;
-- expect: $60.00
DROP TABLE money_test;

-- === L1: SQLSTATE fixes ===

-- get_byte out of range -> 2202E
-- SELECT get_byte('\x01'::bytea, 5);
-- expect: ERROR 2202E

-- set_byte out of range -> 2202E
-- SELECT set_byte('\x01'::bytea, 5, 0);
-- expect: ERROR 2202E

-- Negative substring length -> 22011
-- SELECT substring('hello' from 1 for -1);
-- expect: ERROR 22011

-- chr(0) -> 54000
-- SELECT chr(0);
-- expect: ERROR 54000

-- === L2: bytea substring negative-start clamping ===

SELECT substring('\x123456'::bytea from -1 for 3);
-- expect: \x12

SELECT substring('\x0102030405'::bytea from 0 for 3);
-- expect: \x0102

-- === L6: multidim array casts ===

SELECT ARRAY[[1,2],[3,4]]::int8[];
-- expect: {{1,2},{3,4}}

SELECT ARRAY[1.5,'NaN']::float8[];
-- expect: {1.5,NaN}
