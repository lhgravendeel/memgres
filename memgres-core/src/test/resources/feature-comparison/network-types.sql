-- H18: inet/cidr input validation and normalization
-- H19: inet operators (arithmetic, containment, equality, ordering)
-- H20: Network functions (IPv6 support)
-- H21: macaddr/macaddr8 normalization and operators
-- L3: inet ordering and display

-- H18: IPv6 canonicalization
-- begin-expected
-- columns: result
-- row: 2001:db8::1
-- end-expected
SELECT host('2001:0db8:0000:0000:0000:0000:0000:0001'::inet) AS result;

-- H18: cidr host bit zeroing on inet->cidr cast
-- begin-expected
-- columns: result
-- row: 192.168.1.0/24
-- end-expected
SELECT ('192.168.1.5/24'::inet::cidr)::text AS result;

-- H18: cidr rejects non-zero host bits
-- begin-expected-error
-- error-code: 22P02
-- end-expected
SELECT '192.168.1.5/24'::cidr;

-- H19: inet + integer arithmetic
-- begin-expected
-- columns: result
-- row: 192.168.1.11/32
-- end-expected
SELECT ('192.168.1.1'::inet + 10)::text AS result;

-- H19: inet - integer arithmetic
-- begin-expected
-- columns: result
-- row: 192.168.1.1/32
-- end-expected
SELECT ('192.168.1.11'::inet - 10)::text AS result;

-- H19: inet - inet difference
-- begin-expected
-- columns: result
-- row: 10
-- end-expected
SELECT '192.168.1.11'::inet - '192.168.1.1'::inet AS result;

-- H19: inet && overlap
-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT ('192.168.1.0/24'::inet && '192.168.1.128/25'::inet)::text AS result;

-- H19: equality with /32 default
-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT ('192.168.1.1'::inet = '192.168.1.1/32'::inet)::text AS result;

-- H19: containment operators
-- begin-expected
-- columns: contained|contains|contains_eq|contained_eq
-- row: t|t|t|t
-- end-expected
SELECT
    ('192.168.1.5'::inet << '192.168.1.0/24'::inet)::text AS contained,
    ('192.168.1.0/24'::inet >> '192.168.1.5/32'::inet)::text AS contains,
    ('192.168.1.0/24'::inet >>= '192.168.1.0/24'::inet)::text AS contains_eq,
    ('192.168.1.0/24'::inet <<= '192.168.0.0/16'::inet)::text AS contained_eq;

-- H19: inet * inet rejected
-- begin-expected-error
-- error-code: 42883
-- end-expected
SELECT '192.168.1.1'::inet * '10.0.0.1'::inet;

-- H20: netmask IPv6
-- begin-expected
-- columns: result
-- row: ffff:ffff:ffff:ffff::
-- end-expected
SELECT netmask('2001:db8::1/64'::inet) AS result;

-- H20: network IPv6
-- begin-expected
-- columns: result
-- row: 2001:db8::/32
-- end-expected
SELECT network('2001:db8::1/32'::inet)::text AS result;

-- H20: inet_merge
-- begin-expected
-- columns: result
-- row: 192.168.0.0/22
-- end-expected
SELECT inet_merge('192.168.1.0/24'::inet, '192.168.2.0/24'::inet)::text AS result;

-- H20: family function
-- begin-expected
-- columns: v4|v6
-- row: 4|6
-- end-expected
SELECT family('192.168.1.1'::inet) AS v4, family('::1'::inet) AS v6;

-- H20: abbrev for inet and cidr
-- begin-expected
-- columns: inet_abbrev|cidr_abbrev
-- row: 192.168.1.1|10/8
-- end-expected
SELECT abbrev('192.168.1.1/32'::inet) AS inet_abbrev, abbrev('10.0.0.0/8'::cidr) AS cidr_abbrev;

-- H20: text() function always shows prefix (same as ::text cast)
-- begin-expected
-- columns: result
-- row: 192.168.1.1/32
-- end-expected
SELECT text('192.168.1.1'::inet) AS result;

-- H20: hostmask
-- begin-expected
-- columns: result
-- row: 0.0.0.255
-- end-expected
SELECT hostmask('192.168.1.0/24'::inet) AS result;

-- H21: macaddr normalization from different formats
-- begin-expected
-- columns: colon|dash|dot
-- row: 12:34:56:78:90:ab|12:34:56:78:90:ab|12:34:56:78:90:ab
-- end-expected
SELECT
    '12:34:56:78:90:AB'::macaddr::text AS colon,
    '12-34-56-78-90-AB'::macaddr::text AS dash,
    '1234.5678.90AB'::macaddr::text AS dot;

-- H21: macaddr equality across formats
-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT ('12:34:56:78:90:ab'::macaddr = '12-34-56-78-90-AB'::macaddr)::text AS result;

-- H21: macaddr bitwise NOT
-- begin-expected
-- columns: result
-- row: ed:cb:a9:87:6f:54
-- end-expected
SELECT (~'12:34:56:78:90:ab'::macaddr)::text AS result;

-- H21: macaddr bitwise AND
-- begin-expected
-- columns: result
-- row: 12:34:56:78:90:ab
-- end-expected
SELECT ('ff:ff:ff:ff:ff:ff'::macaddr & '12:34:56:78:90:ab'::macaddr)::text AS result;

-- H21: macaddr trunc
-- begin-expected
-- columns: result
-- row: 12:34:56:00:00:00
-- end-expected
SELECT trunc('12:34:56:78:90:ab'::macaddr)::text AS result;

-- H21: macaddr to macaddr8 (EUI-48 -> EUI-64 with ff:fe)
-- begin-expected
-- columns: result
-- row: 12:34:56:ff:fe:78:90:ab
-- end-expected
SELECT macaddr8('12:34:56:78:90:ab'::macaddr)::text AS result;

-- H21: macaddr8 normalization
-- begin-expected
-- columns: result
-- row: 12:34:56:78:90:ab:cd:ef
-- end-expected
SELECT '12:34:56:78:90:AB:CD:EF'::macaddr8::text AS result;

-- H21: macaddr8_set7bit
-- begin-expected
-- columns: result
-- row: 02:34:56:78:90:ab:cd:ef
-- end-expected
SELECT macaddr8_set7bit('00:34:56:78:90:ab:cd:ef'::macaddr8)::text AS result;

-- H21: macaddr8 to macaddr (removes ff:fe)
-- begin-expected
-- columns: result
-- row: 12:34:56:78:90:ab
-- end-expected
SELECT ('12:34:56:ff:fe:78:90:ab'::macaddr8::macaddr)::text AS result;

-- L3: inet::text always includes prefix (PG network_show behavior)
-- begin-expected
-- columns: result
-- row: 192.168.1.1/32
-- end-expected
SELECT '192.168.1.1'::inet::text AS result;

-- L3: inet display with explicit mask
-- begin-expected
-- columns: result
-- row: 192.168.1.1/24
-- end-expected
SELECT '192.168.1.1/24'::inet::text AS result;

-- L3: cross-family containment returns false
-- begin-expected
-- columns: result
-- row: f
-- end-expected
SELECT ('1.2.3.4'::inet << '::/0'::inet)::text AS result;

-- Cleanup (none needed — no tables created)
