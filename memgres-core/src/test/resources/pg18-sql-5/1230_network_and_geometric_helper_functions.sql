DROP SCHEMA IF EXISTS test_1230 CASCADE;
CREATE SCHEMA test_1230;
SET search_path TO test_1230;

CREATE TABLE net_points (
    id integer PRIMARY KEY,
    addr inet NOT NULL,
    block cidr NOT NULL,
    pt point NOT NULL
);

INSERT INTO net_points VALUES
(1, '192.168.1.10', '192.168.1.0/24', '(1,1)'),
(2, '192.168.2.20', '192.168.2.0/24', '(3,4)'),
(3, '10.0.0.5',     '10.0.0.0/8',     '(0,0)');

-- begin-expected
-- columns: id,host_text,mask_len
-- row: 1|192.168.1.10|24
-- row: 2|192.168.2.20|24
-- row: 3|10.0.0.5|8
-- end-expected
SELECT id, host(addr) AS host_text, masklen(block) AS mask_len
FROM net_points
ORDER BY id;

-- begin-expected
-- columns: id,in_subnet
-- row: 1|t
-- row: 2|f
-- row: 3|f
-- end-expected
SELECT id, addr << inet '192.168.1.0/24' AS in_subnet
FROM net_points
ORDER BY id;

-- begin-expected
-- columns: id,distance
-- row: 1|1.4142135623730951
-- row: 2|5
-- row: 3|0
-- end-expected
SELECT id, pt <-> point '(0,0)' AS distance
FROM net_points
ORDER BY id;

-- begin-expected
-- columns: network_text
-- row: 10.0.0.0/8
-- row: 192.168.1.0/24
-- row: 192.168.2.0/24
-- end-expected
SELECT network(block)::text AS network_text
FROM net_points
ORDER BY network_text;

