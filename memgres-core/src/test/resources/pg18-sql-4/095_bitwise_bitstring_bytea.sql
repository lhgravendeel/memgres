DROP SCHEMA IF EXISTS test_095 CASCADE;
CREATE SCHEMA test_095;
SET search_path TO test_095;

CREATE TABLE bits_and_bytes (
    id integer PRIMARY KEY,
    x integer NOT NULL,
    y integer NOT NULL,
    bfix bit(4) NOT NULL,
    bvar bit varying(8) NOT NULL,
    payload bytea NOT NULL
);

INSERT INTO bits_and_bytes(id, x, y, bfix, bvar, payload) VALUES
    (1, 13, 10, B'1010', B'1101',   '\xDEADBEEF'),
    (2, 6,  3,  B'0011', B'001101', '\x00FF10');

-- begin-expected
-- columns: id|bit_and|bit_or|bit_xor|bit_not|lshift|rshift
-- row: 1|8|15|7|-14|52|6
-- row: 2|2|7|5|-7|24|3
-- end-expected
SELECT id,
       x & y AS bit_and,
       x | y AS bit_or,
       x # y AS bit_xor,
       ~x AS bit_not,
       x << 2 AS lshift,
       x >> 1 AS rshift
FROM bits_and_bytes
ORDER BY id;

-- begin-expected
-- columns: id|bfix_text|bvar_text|bfix_and|bfix_or|bfix_xor|bfix_not|concat_bits
-- row: 1|1010|1101|1000|1111|0111|0101|10101101
-- row: 2|0011|0011|0011|0011|0000|1100|00110011
-- end-expected
SELECT id,
       bfix::text AS bfix_text,
       substring(bvar::text FROM 1 FOR 4) AS bvar_text,
       (bfix & substring(bvar::bit(4) FROM 1 FOR 4))::text AS bfix_and,
       (bfix | substring(bvar::bit(4) FROM 1 FOR 4))::text AS bfix_or,
       (bfix # substring(bvar::bit(4) FROM 1 FOR 4))::text AS bfix_xor,
       (~bfix)::text AS bfix_not,
       (bfix || substring(bvar::text FROM 1 FOR 4))::text AS concat_bits
FROM bits_and_bytes
ORDER BY id;

-- begin-expected
-- columns: id|get_bit_0|get_bit_3|set_bit_1|octets|hex_payload|escaped_payload
-- row: 1|1|1|1110|4|deadbeef|\\336\\255\\276\\357
-- row: 2|0|0|0111|3|00ff10|\\000\\377\020
-- end-expected
SELECT id,
       get_bit(payload, 0) AS get_bit_0,
       get_bit(payload, 3) AS get_bit_3,
       set_bit(bfix::varbit, 1, 1)::text AS set_bit_1,
       octet_length(payload) AS octets,
       encode(payload, 'hex') AS hex_payload,
       encode(payload, 'escape') AS escaped_payload
FROM bits_and_bytes
ORDER BY id;

-- begin-expected
-- columns: id|decoded_hex_matches|decoded_escape_hex
-- row: 1|t|616263
-- row: 2|t|313233
-- end-expected
SELECT id,
       decode(encode(payload, 'hex'), 'hex') = payload AS decoded_hex_matches,
       encode(decode(CASE WHEN id = 1 THEN 'abc' ELSE '123' END, 'escape'), 'hex') AS decoded_escape_hex
FROM bits_and_bytes
ORDER BY id;

DROP SCHEMA test_095 CASCADE;
