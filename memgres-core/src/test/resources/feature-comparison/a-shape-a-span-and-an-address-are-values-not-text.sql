-- ============================================================================
-- -- A shape, a span and an address are values, and their text is only how they print.
-- --
-- -- Every one of these types has a reader of its own, and the reader has to consume the whole
-- -- literal: a box written with three corners is not a box, and a near-number is not a number.
-- -- The reader runs on the way into a column too, so what a table holds is the shape and not the
-- -- characters -- which is what makes a box equal to the same box written the other way round.
-- -- Comparison is the type's own: two circles by their area, two lines by their coefficients
-- -- scaled, two paths by how many points they hold, and coordinates to PostgreSQL's tolerance
-- -- rather than to the last bit. A span carries its fields in the widths they are stored in and
-- -- says so when they will not fit, and each interval style writes a sign the way its own reader
-- -- will read it back. An encoding argument decides what bytes spell, and a codec is
-- -- PostgreSQL's rather than the JDK's.
--
-- ============================================================================

-- ============================================================================
-- 1. A literal is the whole of what it says
-- ============================================================================
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type box: "(1,2),(3,4),(5,6)"
-- end-expected-error
SELECT '(1,2),(3,4),(5,6)'::box::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type line: "{1,2,3,4}"
-- end-expected-error
SELECT '{1,2,3,4}'::line::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(1,2),3,4>"
-- end-expected-error
SELECT '<(1,2),3,4>'::circle::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type lseg: "[(1e,2),(3,4)]"
-- end-expected-error
SELECT '[(1e,2),(3,4)]'::lseg::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type polygon: "((1e,2),(3,4),(5,6))"
-- end-expected-error
SELECT '((1e,2),(3,4),(5,6))'::polygon::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(1e,2),3>"
-- end-expected-error
SELECT '<(1e,2),3>'::circle::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type point: "(1,2)x"
-- end-expected-error
SELECT '(1,2)x'::point::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type point: "((1,2))"
-- end-expected-error
SELECT '((1,2))'::point::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type box: "[(0,0),(1,1)]"
-- end-expected-error
SELECT '[(0,0),(1,1)]'::box::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(1,2),-3>"
-- end-expected-error
SELECT '<(1,2),-3>'::circle::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type path: "[]"
-- end-expected-error
SELECT '[]'::path::text AS a;
-- begin-expected
-- columns: a
-- row: (1,2)
-- end-expected
SELECT ' (1,2) '::point::text AS a;
-- begin-expected
-- columns: a
-- row: (1,2)
-- end-expected
SELECT '(1,2)'::point::text AS a;
-- begin-expected
-- columns: a
-- row: (1,2)
-- end-expected
SELECT '1,2'::point::text AS a;
-- begin-expected
-- columns: a
-- row: [(0,0),(1,1)]
-- end-expected
SELECT '(0,0),(1,1)'::lseg::text AS a;
-- begin-expected
-- columns: a
-- row: (1,1),(0,0)
-- end-expected
SELECT '0,0,1,1'::box::text AS a;
-- begin-expected
-- columns: a
-- row: ((0,0),(1,1),(2,2))
-- end-expected
SELECT '((0,0),(1,1),(2,2))'::path::text AS a;
-- begin-expected
-- columns: a
-- row: ((0,0),(1,1),(2,2))
-- end-expected
SELECT '(0,0),(1,1),(2,2)'::polygon::text AS a;
-- begin-expected
-- columns: a
-- row: <(1,2),3>
-- end-expected
SELECT '((1,2),3)'::circle::text AS a;
-- begin-expected
-- columns: a
-- row: <(1,2),3>
-- end-expected
SELECT '1,2,3'::circle::text AS a;
-- begin-expected
-- columns: a
-- row: (NaN,0)
-- end-expected
SELECT '(NaN,0)'::point::text AS a;
-- begin-expected
-- columns: a
-- row: (Infinity,0)
-- end-expected
SELECT '(inf,0)'::point::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type point: "(1d,2)"
-- end-expected-error
SELECT '(1d,2)'::point::text AS a;

-- ============================================================================
-- 2. A column holds the shape, so the same shape compares equal however written
-- ============================================================================
DROP TABLE IF EXISTS zzsh_g CASCADE;
CREATE TABLE zzsh_g (id int, b box, c circle);
INSERT INTO zzsh_g VALUES (1, '(0,0),(1,1)', '<(1,2),3>'), (4, '(0,1),(1,0)', '<(0,0),1>');
-- begin-expected
-- columns: id | b | text
-- row: 1 | (1,1),(0,0) | true
-- row: 4 | (1,1),(0,0) | true
-- end-expected
SELECT id, b::text, (b = '(1,1),(0,0)'::box)::text FROM zzsh_g ORDER BY id;
DROP TABLE zzsh_g;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('<(0,0),1>'::circle = '<(9,9),1>'::circle)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('{1,1,0}'::line = '{2,2,0}'::line)::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('[(0,0),(1,1)]'::path < '[(0,0),(2,2)]'::path)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('(NaN,0)'::point ~= '(NaN,0)'::point)::text AS a;
-- begin-expected
-- columns: a
-- row: {-1,0,0}
-- end-expected
SELECT '[(0,0),(0,1)]'::line::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid line specification: must be two distinct points
-- end-expected-error
SELECT '[(0,0),(0,0)]'::line::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid line specification: must be two distinct points
-- end-expected-error
SELECT '[(0,0),(0.0000005,0)]'::line::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid line specification: must be two distinct points
-- end-expected-error
SELECT line(point(0,0), point(0,0))::text AS a;
-- begin-expected
-- columns: a
-- row: <(1.3333333333333333,0.6666666666666666),1.308077670527261>
-- end-expected
SELECT circle('((0,0),(2,0),(2,2))'::polygon)::text AS a;
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT slope(point '(1,0)', point '(0,0)')::text AS a;
-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT slope(point '(0,0)', point '(0.0000005,5)')::text AS a;
-- begin-expected
-- columns: a
-- row: (1000000,1000000)
-- end-expected
SELECT (point '(1,1)' / point '(0.000001,0)')::text AS a;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT npoints(path '[(0,0),(1,1),(2,2)]') AS a;

-- expected-divergence: the crossing is the origin either way, and the two engines name the same
-- point. Which of the two zeros it is -- the positive one or the negative one -- falls out of
-- the order the multiplications and subtractions happen to be written in inside the formula,
-- and that is a property of the arithmetic rather than of the two lines.
SELECT ('{1,-1,0}'::line # '{1,1,0}'::line)::text AS a;

-- expected-divergence: how an indented serialisation lays out the whitespace around content is
-- the formatter's own choice; the document it writes is the same document either way.
SELECT ('[' || XMLSERIALIZE(CONTENT ' <a/> '::xml AS text INDENT) || ']') AS a;

-- expected-divergence: the sentence is fixed and is the same one, and the parser's account of
-- what went wrong is the detail beneath it -- written by libxml on the reference server and by
-- the JDK's parser here, so the wording differs while the complaint does not.
SELECT 'a<b'::xml AS a;

-- ============================================================================
-- 3. xml has no operators at all
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: xml = xml
-- end-expected-error
SELECT ('<a/>'::xml = '<a/>'::xml)::text AS a;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: xml < xml
-- end-expected-error
SELECT ('<a/>'::xml < '<b/>'::xml)::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT (NULL::xml IS DOCUMENT)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('<a/>'::xml IS DOCUMENT)::text AS a;
-- begin-expected
-- columns: a
-- row: <a/>
-- end-expected
SELECT xmlroot('<a/>'::xml, version '1.0')::text AS a;
-- begin-expected
-- columns: a
-- row: <?xml version="2.0"?><a/>
-- end-expected
SELECT xmlroot('<a/>'::xml, version '2.0')::text AS a;
-- begin-expected
-- columns: a
-- row: <?xml version="1.0" standalone="yes"?><a/>
-- end-expected
SELECT xmlroot('<a/>'::xml, version '1.0', standalone yes)::text AS a;
-- begin-expected
-- columns: a
-- row: <f><root><row/></root></f>
-- end-expected
SELECT xmlforest('<root><row/></root>'::xml AS f)::text AS a;
-- begin-expected
-- columns: a
-- row: <foo att_x003C_r="a&amp;b"/>
-- end-expected
SELECT xmlelement(name foo, xmlattributes('a&b' as "att<r"))::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: XML attribute name "a" appears more than once
-- end-expected-error
SELECT xmlelement(name foo, xmlattributes(1 as a, 2 as a))::text AS a;

-- ============================================================================
-- 4. A span holds its fields in the widths it stores them in
-- ============================================================================
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT make_interval(years => 200000000)::text AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT make_interval(weeks => 400000000)::text AS a;
-- begin-expected
-- columns: a
-- row: 178956970 years 7 mons
-- end-expected
SELECT make_interval(months => 2147483647)::text AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT make_interval(years => 178956970, months => 8)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (interval '1000000000 mons' > interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (interval '2000000000 mons' > interval '1000000000 mons')::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT make_interval(NULL)::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT make_date(NULL, 1, 1)::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT make_date(2020, NULL, 1)::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT make_time(NULL, 1, 1)::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT make_timestamptz(2020, 1, 1, 0, 0, 0, NULL)::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT to_number('123', NULL)::text AS a;

-- ============================================================================
-- 5. Each interval style writes a sign its own reader reads back
-- ============================================================================
SET intervalstyle = 'iso_8601';
-- begin-expected
-- columns: a
-- row: PT-0.5S
-- end-expected
SELECT interval '-0.5 seconds' AS a;
-- begin-expected
-- columns: a
-- row: PT-1H-0.5S
-- end-expected
SELECT interval '-1 hour -0.5 seconds' AS a;
-- begin-expected
-- columns: a
-- row: P-1DT-0.25S
-- end-expected
SELECT interval '-1 day -0.25 seconds' AS a;
-- begin-expected
-- columns: a
-- row: PT-1H-30M
-- end-expected
SELECT interval '-1 hour -30 minutes' AS a;
-- begin-expected
-- columns: a
-- row: P-1Y-2M
-- end-expected
SELECT interval '-1 year -2 mons' AS a;
SET intervalstyle = 'sql_standard';
-- begin-expected
-- columns: a
-- row: -0-1
-- end-expected
SELECT interval '-1 month' AS a;
-- begin-expected
-- columns: a
-- row: -1-1
-- end-expected
SELECT interval '-13 months' AS a;
-- begin-expected
-- columns: a
-- row: 0-10
-- end-expected
SELECT interval '1 year -2 months' AS a;
-- begin-expected
-- columns: a
-- row: -1 2:03:00
-- end-expected
SELECT interval '-1 day -2 hours -3 minutes' AS a;
-- begin-expected
-- columns: a
-- row: +0-0 +1 -2:00:00
-- end-expected
SELECT interval '1 day -2 hours' AS a;
-- begin-expected
-- columns: a
-- row: -1-0 -1 -1:00:00
-- end-expected
SELECT interval '-1 year -1 day -1 hour' AS a;
SET intervalstyle = 'postgres_verbose';
-- begin-expected
-- columns: a
-- row: @ 1 mon ago
-- end-expected
SELECT interval '-1 month' AS a;
-- begin-expected
-- columns: a
-- row: @ 1 day 2 hours ago
-- end-expected
SELECT interval '-1 day -2 hours' AS a;
-- begin-expected
-- columns: a
-- row: @ 1 day -2 hours
-- end-expected
SELECT interval '1 day -2 hours' AS a;
-- begin-expected
-- columns: a
-- row: @ 1 year 2 mons 3 days 4 hours 5 mins 6 secs ago
-- end-expected
SELECT interval '-1 year -2 mons -3 days -04:05:06' AS a;
SET intervalstyle = 'postgres';
-- begin-expected
-- columns: a
-- row: -10 mons
-- end-expected
SELECT interval '-1 year 2 months' AS a;

-- ============================================================================
-- 6. A range created by CREATE TYPE is a type
-- ============================================================================
DROP TYPE IF EXISTS zzsh_r CASCADE;
DROP TYPE IF EXISTS zzsh_r2 CASCADE;
CREATE TYPE zzsh_r AS RANGE (SUBTYPE = int);
DROP TABLE IF EXISTS zzsh_rt CASCADE;
CREATE TABLE zzsh_rt (i int);
ALTER TABLE zzsh_rt ADD COLUMN b zzsh_r;
-- begin-expected
-- columns: format_type
-- row: zzsh_r
-- end-expected
SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'zzsh_rt'::regclass AND attname = 'b';
ALTER TYPE zzsh_r RENAME TO zzsh_r2;
-- begin-expected
-- columns: typname
-- row: zzsh_r2
-- row: zzsh_r_multirange
-- end-expected
SELECT typname FROM pg_type WHERE typname IN ('zzsh_r','zzsh_r2','zzsh_r_multirange','zzsh_r2_multirange') ORDER BY typname;
-- begin-expected
-- columns: a
-- row: [1,5)
-- end-expected
SELECT '[1,5)'::zzsh_r2::text AS a;
DROP TABLE zzsh_rt;
DROP TYPE zzsh_r2;

-- ============================================================================
-- 7. An address is read by the spellings PostgreSQL lists
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 10.0.0.0/8
-- end-expected
SELECT '10/8'::inet::text AS a;
-- begin-expected
-- columns: a
-- row: 10.1.0.0/16
-- end-expected
SELECT '10.1/16'::inet::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type inet: "192.168.1.5 "
-- end-expected-error
SELECT '192.168.1.5 '::inet::text AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type inet: " 192.168.1.5"
-- end-expected-error
SELECT ' 192.168.1.5'::inet::text AS a;
-- begin-expected
-- columns: a
-- row: 192.168.1.5/32
-- end-expected
SELECT set_masklen('192.168.1.5/24'::inet, -1)::text AS a;
-- begin-expected
-- columns: a
-- row: 32
-- end-expected
SELECT masklen(set_masklen('192.168.1.5/24'::inet, -1)) AS a;
-- begin-expected
-- columns: a
-- row: 08:00:2b:01:02:03
-- end-expected
SELECT '0800-2b01-0203'::macaddr::text AS a;
-- begin-expected
-- columns: a
-- row: 08:00:2b:01:02:03:04:05
-- end-expected
SELECT '08002b:0102030405'::macaddr8::text AS a;
-- begin-expected
-- columns: a
-- row: 08:00:2b:01:02:03:04:05
-- end-expected
SELECT '08002b-0102030405'::macaddr8::text AS a;
-- begin-expected
-- columns: a
-- row: 08:00:2b:01:02:03:04:05
-- end-expected
SELECT '0800-2b01-0203-0405'::macaddr8::text AS a;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: invalid octet value in "macaddr" value: "00:11:22:33:44:-6"
-- end-expected-error
SELECT '00:11:22:33:44:-6'::macaddr::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot AND inet values of different sizes
-- end-expected-error
SELECT ('192.168.1.5'::inet & '::1'::inet)::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot OR inet values of different sizes
-- end-expected-error
SELECT ('192.168.1.5'::inet | '::1'::inet)::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot subtract inet values of different sizes
-- end-expected-error
SELECT ('192.168.1.5'::inet - '::1'::inet)::text AS a;

-- ============================================================================
-- 8. A codec is PostgreSQL's, and an encoding name decides what bytes spell
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 77
-- end-expected
SELECT length(encode(decode(repeat('61', 57), 'hex'), 'base64')) AS a;
-- begin-expected
-- columns: a
-- row: 1353
-- end-expected
SELECT length(encode(decode(repeat('61', 1000), 'hex'), 'base64')) AS a;
-- begin-expected
-- columns: a
-- row: 61626364
-- end-expected
SELECT encode(decode(E'YWJj\nZA==', 'base64'), 'hex') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid base64 end sequence
-- end-expected-error
SELECT encode(decode('YWJjZA','base64'),'hex') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid symbol "!" found while decoding base64 sequence
-- end-expected-error
SELECT encode(decode('!!!!','base64'),'hex') AS a;
-- begin-expected
-- columns: a
-- row: 1234
-- end-expected
SELECT encode(decode('12 34', 'hex'), 'hex') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid hexadecimal digit: "x"
-- end-expected-error
SELECT encode(decode('xy','hex'),'hex') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid hexadecimal data: odd number of digits
-- end-expected-error
SELECT encode(decode('123','hex'),'hex') AS a;
-- begin-expected
-- columns: a
-- row: c3a9
-- end-expected
SELECT encode(decode('é', 'escape'), 'hex') AS a;
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT length(encode('\x0102030405'::bytea, 'escape')) AS a;
-- begin-expected
-- columns: a
-- row: \000
-- end-expected
SELECT encode('\x00'::bytea,'escape') AS a;
-- begin-expected
-- columns: a
-- row: \377
-- end-expected
SELECT encode('\xff'::bytea,'escape') AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT encode('\x0102'::bytea, NULL) AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT decode('abcd', NULL) AS a;
-- begin-expected
-- columns: a
-- row: é
-- end-expected
SELECT convert_from('\xe9'::bytea, 'LATIN1') AS a;
-- begin-expected
-- columns: a
-- row: \x616263
-- end-expected
SELECT convert_to('abc','SQL_ASCII') AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT convert('abc'::bytea, NULL, 'UTF8') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid source encoding name "BOGUSENC"
-- end-expected-error
SELECT convert('abc'::bytea, 'BOGUSENC', 'UTF8') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid destination encoding name "BOGUSENC"
-- end-expected-error
SELECT convert('abc'::bytea, 'UTF8', 'BOGUSENC') AS a;
-- begin-expected
-- columns: a
-- row: c3a9
-- end-expected
SELECT encode(convert('\xe9'::bytea, 'LATIN1', 'UTF8'),'hex') AS a;
-- begin-expected
-- columns: a
-- row: KarACl
-- end-expected
SELECT to_ascii('Karél', 'LATIN1') AS a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: NOSUCHENC is not a valid encoding name
-- end-expected-error
SELECT to_ascii('abc', 'NOSUCHENC') AS a;
-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT length('jose', 'UTF8') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid encoding name "BOGUS"
-- end-expected-error
SELECT length('jose', 'BOGUS') AS a;
-- begin-expected
-- columns: a
-- row: bytea
-- end-expected
SELECT pg_typeof(sha256('abc'::bytea))::text AS a;
-- begin-expected
-- columns: a
-- row: ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
-- end-expected
SELECT encode(sha256('abc'::bytea),'hex') AS a;
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: index -1 out of valid range, 0..7
-- end-expected-error
SELECT get_bit('\xff'::bytea, -1) AS a;
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: index -1 out of valid range, 0..15
-- end-expected-error
SELECT get_bit('\x00ff'::bytea, -1) AS a;
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: index 8 out of valid range, 0..7
-- end-expected-error
SELECT get_bit('\xff'::bytea, 8) AS a;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function get_byte(bytea, bigint) does not exist
-- end-expected-error
SELECT get_byte('\x0102'::bytea, 4294967296) AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT get_byte('\x0102'::bytea, NULL) AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT get_bit('\xff'::bytea, NULL) AS a;
