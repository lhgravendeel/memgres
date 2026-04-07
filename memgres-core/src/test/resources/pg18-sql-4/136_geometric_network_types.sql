DROP SCHEMA IF EXISTS test_136 CASCADE;
CREATE SCHEMA test_136;
SET search_path TO test_136;

CREATE TABLE geo_net (
    id integer PRIMARY KEY,
    p1 point NOT NULL,
    p2 point NOT NULL,
    bx box NOT NULL,
    cir circle NOT NULL,
    host inet NOT NULL,
    net cidr NOT NULL,
    mac macaddr NOT NULL
);

INSERT INTO geo_net(id, p1, p2, bx, cir, host, net, mac) VALUES
    (1, point(0,0), point(3,4), box(point(0,0), point(5,5)), circle(point(0,0), 5), '192.168.1.10', '192.168.1.0/24', '08:00:2b:01:02:03'),
    (2, point(1,1), point(-2,5), box(point(-3,-3), point(2,2)), circle(point(1,1), 2), '10.5.6.7', '10.0.0.0/8', '08:00:2b:01:02:04');

-- begin-expected
-- columns: id|distance|point_add|point_sub
-- row: 1|5|(3,4)|(-3,-4)
-- row: 2|5|(-1,6)|(3,-4)
-- end-expected
SELECT id,
       p1 <-> p2 AS distance,
       (p1 + p2)::text AS point_add,
       (p1 - p2)::text AS point_sub
FROM geo_net
ORDER BY id;

-- begin-expected
-- columns: id|box_contains_p1|circle_contains_p2|box_area
-- row: 1|t|t|25
-- row: 2|t|f|25
-- end-expected
SELECT id,
       bx @> p1 AS box_contains_p1,
       cir @> p2 AS circle_contains_p2,
       area(bx) AS box_area
FROM geo_net
ORDER BY id;

-- begin-expected
-- columns: id|center_point|radius_val|diameter_val
-- row: 1|(0,0)|5|10
-- row: 2|(1,1)|2|4
-- end-expected
SELECT id,
       center(cir)::text AS center_point,
       radius(cir) AS radius_val,
       diameter(cir) AS diameter_val
FROM geo_net
ORDER BY id;

-- begin-expected
-- columns: id|same_subnet|contained_by_net|family_no|masked_host|network_part|broadcast_addr
-- row: 1|t|t|4|192.168.1.10/24|192.168.1.0/24|192.168.1.255/24
-- row: 2|t|t|4|10.5.6.7/8|10.0.0.0/8|10.255.255.255/8
-- end-expected
SELECT id,
       host << net AS same_subnet,
       host <<= net AS contained_by_net,
       family(host) AS family_no,
       set_masklen(host, masklen(net)) AS masked_host,
       network(host) AS network_part,
       broadcast(net) AS broadcast_addr
FROM geo_net
ORDER BY id;

-- begin-expected
-- columns: id|host_text|host_only|mask_len|mac_text
-- row: 1|192.168.1.10/32|192.168.1.10|24|08:00:2b:01:02:03
-- row: 2|10.5.6.7/32|10.5.6.7|8|08:00:2b:01:02:04
-- end-expected
SELECT id,
       text(host) AS host_text,
       host(host) AS host_only,
       masklen(net) AS mask_len,
       mac::text AS mac_text
FROM geo_net
ORDER BY id;

DROP SCHEMA test_136 CASCADE;
