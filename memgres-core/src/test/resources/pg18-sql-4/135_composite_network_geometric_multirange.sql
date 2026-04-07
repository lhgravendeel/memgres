DROP SCHEMA IF EXISTS test_135 CASCADE;
CREATE SCHEMA test_135;
SET search_path TO test_135;

CREATE TYPE pair_t AS (
    left_n integer,
    right_n integer
);

CREATE TABLE exotic_types (
    id integer PRIMARY KEY,
    pair_val pair_t NOT NULL,
    host inet NOT NULL,
    net cidr NOT NULL,
    loc point NOT NULL,
    span int4multirange NOT NULL,
    uid uuid NOT NULL,
    bits bit(4) NOT NULL,
    data bytea NOT NULL
);

INSERT INTO exotic_types(id, pair_val, host, net, loc, span, uid, bits, data) VALUES
    (1, ROW(1, 10), '192.168.1.5', '192.168.1.0/24', point(1,2), '{[1,4),[10,12)}', '11111111-1111-1111-1111-111111111111', B'1010', '\xDEADBEEF'),
    (2, ROW(2, 20), '10.0.0.8', '10.0.0.0/8', point(-1,3), '{[5,8)}', '22222222-2222-2222-2222-222222222222', B'0011', '\x00FF');

-- begin-expected
-- columns: id|left_n|right_n
-- row: 1|1|10
-- row: 2|2|20
-- end-expected
SELECT id, (pair_val).left_n AS left_n, (pair_val).right_n AS right_n
FROM exotic_types
ORDER BY id;

-- begin-expected
-- columns: id|same_subnet|contains_host
-- row: 1|t|t
-- row: 2|t|t
-- end-expected
SELECT id,
       host <<= net AS same_subnet,
       net >>= host AS contains_host
FROM exotic_types
ORDER BY id;

-- begin-expected
-- columns: id|distance_origin
-- row: 1|2.23606797749979
-- row: 2|3.1622776601683795
-- end-expected
SELECT id, loc <-> point(0,0) AS distance_origin
FROM exotic_types
ORDER BY id;

-- begin-expected
-- columns: id|contains_3|contains_6
-- row: 1|t|f
-- row: 2|f|t
-- end-expected
SELECT id,
       span @> 3 AS contains_3,
       span @> 6 AS contains_6
FROM exotic_types
ORDER BY id;

-- begin-expected
-- columns: id|uid|bits|data_hex
-- row: 1|11111111-1111-1111-1111-111111111111|1010|deadbeef
-- row: 2|22222222-2222-2222-2222-222222222222|0011|00ff
-- end-expected
SELECT id, uid::text AS uid, bits::text AS bits, encode(data, 'hex') AS data_hex
FROM exotic_types
ORDER BY id;

DROP SCHEMA test_135 CASCADE;
