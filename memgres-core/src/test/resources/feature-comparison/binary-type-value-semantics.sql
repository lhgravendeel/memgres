-- ============================================================================
-- Feature Comparison: binary and bit-string value semantics
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- bytea, bit and money are values, not opaque objects. Equal bytea values fall
-- into one group, aggregates over them produce a bytea, concat renders them
-- with the type's own output function, and malformed input is rejected.
-- ============================================================================

DROP TABLE IF EXISTS bvs_bytes CASCADE;
DROP TABLE IF EXISTS bvs_bits CASCADE;
DROP TABLE IF EXISTS bvs_vb CASCADE;

CREATE TABLE bvs_bytes (id int PRIMARY KEY, b bytea);
INSERT INTO bvs_bytes VALUES (1, '\x0102'), (2, '\x0102'), (3, '\x03');

-- ============================================================================
-- 1. Equal bytea values are one group
-- ============================================================================

-- begin-expected
-- columns: b | n
-- row: \x0102, 2
-- row: \x03, 1
-- end-expected
SELECT b, count(*) AS n FROM bvs_bytes GROUP BY b ORDER BY b;

-- begin-expected
-- columns: d
-- row: 2
-- end-expected
SELECT count(DISTINCT b) AS d FROM bvs_bytes;

-- begin-expected
-- columns: b
-- row: \x0102
-- row: \x03
-- end-expected
SELECT DISTINCT b FROM bvs_bytes ORDER BY b;

-- ============================================================================
-- 2. Aggregates and concat use the type's own output function
-- ============================================================================

-- begin-expected
-- columns: agg
-- row: \x0102010203
-- end-expected
SELECT string_agg(b, ''::bytea ORDER BY id) AS agg FROM bvs_bytes;

-- begin-expected
-- columns: c1 | c2
-- row: a\x0102, a{1,2}
-- end-expected
SELECT concat('a', '\x0102'::bytea) AS c1, concat('a', ARRAY[1,2]) AS c2;

CREATE TABLE bvs_bits (id int PRIMARY KEY, v bit(5));
INSERT INTO bvs_bits VALUES (1, B'10101'), (2, B'11100');

-- begin-expected
-- columns: a | o | x
-- row: 10100, 11101, 01001
-- end-expected
SELECT bit_and(v)::text AS a, bit_or(v)::text AS o, bit_xor(v)::text AS x FROM bvs_bits;

-- begin-expected
-- columns: a | o | x
-- row: 0, 3, 3
-- end-expected
SELECT bit_and(id) AS a, bit_or(id) AS o, bit_xor(id) AS x FROM bvs_bits;

-- ============================================================================
-- 3. varbit keeps its bits
-- ============================================================================

-- begin-expected
-- columns: v
-- row: 101
-- end-expected
SELECT B'10101'::varbit(3) AS v;

-- begin-expected
-- columns: v
-- row: 1011
-- end-expected
SELECT '1011'::varbit AS v;

CREATE TABLE bvs_vb (id int PRIMARY KEY, v varbit(3));

-- Column assignment still rejects an over-long value
-- begin-expected-error
-- sqlstate: 22001
-- message-like: bit string too long for type bit varying(3)
-- end-expected-error
INSERT INTO bvs_vb VALUES (1, B'10101');

-- ============================================================================
-- 4. Malformed input is rejected
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: invalid octet value
-- end-expected-error
SELECT '00:11:22:33:44:-6'::macaddr;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bytea
-- end-expected-error
SELECT '\q'::bytea;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type money
-- end-expected-error
SELECT '1e3'::money;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: does not exist
-- end-expected-error
SELECT avg(x) FROM (SELECT '1'::money AS x) t;

-- Valid values still parse
-- begin-expected
-- columns: m | mo
-- row: 00:11:22:33:44:55, $234.56
-- end-expected
SELECT '00:11:22:33:44:55'::macaddr AS m, '234.56'::money AS mo;

DROP TABLE bvs_bytes;
DROP TABLE bvs_bits;
DROP TABLE bvs_vb;
