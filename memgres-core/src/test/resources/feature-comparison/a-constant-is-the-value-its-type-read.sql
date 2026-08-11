-- What a stored definition says a constant is: the value its type read, not the text it was
-- typed as.
--   * a time of day is read the way the clock part of a timestamp is -- one digit is enough in
--     any field, the seconds may be left off, a sixtieth second is the next minute -- and the
--     value is written back in full.
--   * an array constant is read brace by brace and written back element by element, so the
--     spacing it was typed with is not part of it.
--   * an integer and a numeric that reads as a fraction are written bare; everything else is
--     quoted and labelled, a negative number included.
--   * a cast to the type an expression already has was dropped by parse analysis and cannot be
--     printed; one that applies a width, or one to another type, stays.
--   * a column default and a CHECK constraint are printed by machinery of their own, and read
--     the same two constants the same way.
-- Every value below was read off PostgreSQL 18. Newlines are written as the two characters
-- backslash-n by the replace() around each call, so one definition fits on one annotated row.

-- setup
CREATE TABLE cvt_v1 (id int, ts timestamp, dt date, iv interval, b boolean, u uuid, jb jsonb, ba bytea, ip inet, mc macaddr, bt bit(8), vb bit varying(8));
CREATE TABLE cvt_n1 (id int, sm smallint, bg bigint, nm numeric, f4 real, f8 double precision);
CREATE TABLE cvt_f1 (id int, sm smallint, bg bigint, nm numeric, nm2 numeric(10,2), f4 real, nme text, vc varchar(10), ch char(5));
CREATE TABLE cvt_tm (id int, tm time, tz timetz);
CREATE TABLE cvt_ar (id int, arr int[], nm numeric[], txa text[], m int[], da date[], e int[]);

-- stmt 1: a time of day is read by the reader a timestamp's clock part is read by
-- begin-expected
-- columns: d
-- row: 03:04:00
-- end-expected
SELECT ('3:4'::time)::text AS d;
-- begin-expected
-- columns: d
-- row: 03:04:05
-- end-expected
SELECT ('3:4:5'::time)::text AS d;
-- begin-expected
-- columns: d
-- row: 03:04:05.5
-- end-expected
SELECT ('3:4:5.500'::time)::text AS d;
-- begin-expected
-- columns: d
-- row: 24:00:00
-- end-expected
SELECT ('23:59:60'::time)::text AS d;
-- begin-expected
-- columns: d
-- row: 03:05:00
-- end-expected
SELECT ('3:4:60'::time)::text AS d;
-- begin-expected
-- columns: d
-- row: 03:04:05.123457
-- end-expected
SELECT ('3:4:5.123456789'::time)::text AS d;
-- begin-expected
-- columns: d
-- row: 03:04:00+02
-- end-expected
SELECT ('3:4+02'::timetz)::text AS d;
-- begin-expected
-- columns: d
-- row: true
-- end-expected
SELECT ('3:4'::time = '03:04:00'::time)::text AS d;
-- begin-expected
-- columns: d
-- row: 24:00:00
-- end-expected
SELECT ('24:00'::time)::text AS d;

-- stmt 2: a field the reader takes but the calendar does not is out of range
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "25:00"
-- end-expected-error
SELECT '25:00'::time AS d;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "3:60"
-- end-expected-error
SELECT '3:60'::time AS d;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "3:4:61"
-- end-expected-error
SELECT '3:4:61'::time AS d;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type time: "3:4:5:6"
-- end-expected-error
SELECT '3:4:5:6'::time AS d;

-- stmt 3: a time constant in a definition prints the value it read
CREATE VIEW cvt_w1 AS SELECT id FROM cvt_tm WHERE tm > '3:4';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_tm\n  WHERE tm > '03:04:00'::time without time zone;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_w1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_w2 AS SELECT id FROM cvt_tm WHERE tm > '3:4:5' AND tm < '3:4:5.500';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_tm\n  WHERE tm > '03:04:05'::time without time zone AND tm < '03:04:05.5'::time without time zone;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_w2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_w3 AS SELECT id FROM cvt_tm WHERE tz > '3:4+02';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_tm\n  WHERE tz > '03:04:00+02'::time with time zone;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_w3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_w4 AS SELECT '3:4'::time AS a, '3:4:5'::time AS b, time '3:4' AS c;
-- begin-expected
-- columns: d
-- row:  SELECT '03:04:00'::time without time zone AS a,\n    '03:04:05'::time without time zone AS b,\n    '03:04:00'::time without time zone AS c;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_w4'::regclass, true), chr(10), '\n') AS d;

-- stmt 4: an array constant is written element by element
CREATE VIEW cvt_a1 AS SELECT id FROM cvt_ar WHERE arr = '{ 1, 2 , 3 }' AND txa = '{ a , b }' AND nm = '{ 01.50, 2 }';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_ar\n  WHERE arr = '{1,2,3}'::integer[] AND txa = '{a,b}'::text[] AND nm = '{1.50,2}'::numeric[];
-- end-expected
SELECT replace(pg_get_viewdef('cvt_a1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_a2 AS SELECT id FROM cvt_ar WHERE txa = '{"x y", z}' AND arr = '{ 007, 8 }' AND arr = '{NULL, 1}';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_ar\n  WHERE txa = '{"x y",z}'::text[] AND arr = '{7,8}'::integer[] AND arr = '{NULL,1}'::integer[];
-- end-expected
SELECT replace(pg_get_viewdef('cvt_a2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_a3 AS SELECT '{ 1, 2 }'::int[] AS a, '{ x, y }'::text[] AS b;
-- begin-expected
-- columns: d
-- row:  SELECT '{1,2}'::integer[] AS a,\n    '{x,y}'::text[] AS b;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_a3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_a4 AS SELECT id FROM cvt_ar WHERE arr @> '{ 1 }';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_ar\n  WHERE arr @> '{1}'::integer[];
-- end-expected
SELECT replace(pg_get_viewdef('cvt_a4'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_a5 AS SELECT id FROM cvt_ar WHERE m = '{ {1,2}, {3,4} }' AND da = '{ 2020-1-2 }' AND e = '{}';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_ar\n  WHERE m = '{{1,2},{3,4}}'::integer[] AND da = '{2020-01-02}'::date[] AND e = '{}'::integer[];
-- end-expected
SELECT replace(pg_get_viewdef('cvt_a5'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_a6 AS SELECT id FROM cvt_ar WHERE arr = '[0:2]={ 1, 2, 3 }';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_ar\n  WHERE arr = '[0:2]={1,2,3}'::integer[];
-- end-expected
SELECT replace(pg_get_viewdef('cvt_a6'::regclass, true), chr(10), '\n') AS d;

-- stmt 5: every other type's own writer makes the text of its value
CREATE VIEW cvt_ts AS SELECT id FROM cvt_v1 WHERE ts > '2020-01-01' AND ts < '2020-1-2 3:4:5.6';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_v1\n  WHERE ts > '2020-01-01 00:00:00'::timestamp without time zone AND ts < '2020-01-02 03:04:05.6'::timestamp without time zone;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_ts'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_dt AS SELECT id FROM cvt_v1 WHERE dt > '2020-1-2' AND iv > '1 day 2 hours 3 min';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_v1\n  WHERE dt > '2020-01-02'::date AND iv > '1 day 02:03:00'::interval;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_dt'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_bool AS SELECT id FROM cvt_v1 WHERE b = 't' OR b = 'yes';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_v1\n  WHERE b = true OR b = true;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_bool'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_val AS SELECT id FROM cvt_v1 WHERE u = '0A0B0C0D-0E0F-1011-1213-141516171819' AND jb = '{"b":1, "a":2}' AND ba = 'abc';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_v1\n  WHERE u = '0a0b0c0d-0e0f-1011-1213-141516171819'::uuid AND jb = '{"a": 2, "b": 1}'::jsonb AND ba = '\x616263'::bytea;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_val'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_net AS SELECT id FROM cvt_v1 WHERE ip = '010.0.0.1/8' AND mc = '08002B010203' AND bt = '10101010' AND vb = '1010';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_v1\n  WHERE ip = '10.0.0.1/8'::inet AND mc = '08:00:2b:01:02:03'::macaddr AND bt = '10101010'::"bit" AND vb = '1010'::bit varying;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_net'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_bit AS SELECT B'1010' AS a, X'0A' AS b FROM cvt_v1;
-- begin-expected
-- columns: d
-- row:  SELECT '1010'::"bit" AS a,\n    '00001010'::"bit" AS b\n   FROM cvt_v1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_bit'::regclass, true), chr(10), '\n') AS d;

-- stmt 6: a number carries the label its text alone would not give
CREATE VIEW cvt_num AS SELECT id FROM cvt_n1 WHERE nm > '01.50' AND id > '007' AND sm > '3' AND bg > '9999999999';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_n1\n  WHERE nm > 1.50 AND id > 7 AND sm > '3'::smallint AND bg > '9999999999'::bigint;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_num'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_num2 AS SELECT id FROM cvt_n1 WHERE f8 > '1.5' AND f4 > '1.5' AND nm > '5' AND nm > 1e3;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_n1\n  WHERE f8 > '1.5'::double precision AND f4 > '1.5'::real AND nm > '5'::numeric AND nm > '1000'::numeric;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_num2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_neg AS SELECT id FROM cvt_n1 WHERE id > -1 AND nm > -1.5;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM cvt_n1\n  WHERE id > '-1'::integer AND nm > '-1.5'::numeric;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_neg'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_cst AS SELECT '2020-1-2'::date AS a, '1.50'::numeric AS b, '007'::int AS c, '1.50'::numeric(10,2) AS d, (-1)::int AS e, 1.5::numeric(10,2) AS f FROM cvt_n1;
-- begin-expected
-- columns: d
-- row:  SELECT '2020-01-02'::date AS a,\n    1.50 AS b,\n    7 AS c,\n    1.50::numeric(10,2) AS d,\n    '-1'::integer AS e,\n    1.5::numeric(10,2) AS f\n   FROM cvt_n1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_cst'::regclass, true), chr(10), '\n') AS d;

-- stmt 7: a cast the expression did not need is not in the stored query
CREATE VIEW cvt_fold AS SELECT upper(nme)::text AS a, nme::text AS b, id::int AS c, (id + 1)::int AS d, length(nme)::int AS e, lower(nme)::varchar AS f FROM cvt_f1;
-- begin-expected
-- columns: d
-- row:  SELECT upper(nme) AS a,\n    nme AS b,\n    id AS c,\n    id + 1 AS d,\n    length(nme) AS e,\n    lower(nme)::character varying AS f\n   FROM cvt_f1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_fold'::regclass, true), chr(10), '\n') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT upper(nme) AS a,\n    nme AS b,\n    id AS c,\n    (id + 1) AS d,\n    length(nme) AS e,\n    (lower(nme))::character varying AS f\n   FROM cvt_f1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_fold'::regclass, false), chr(10), '\n') AS d;
CREATE VIEW cvt_fold2 AS SELECT nm::numeric AS a, nm2::numeric AS b, ch::bpchar AS c, id::numeric AS d, sm::smallint AS e, bg::bigint AS f FROM cvt_f1;
-- begin-expected
-- columns: d
-- row:  SELECT nm AS a,\n    nm2::numeric AS b,\n    ch::bpchar AS c,\n    id::numeric AS d,\n    sm AS e,\n    bg AS f\n   FROM cvt_f1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_fold2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_fold3 AS SELECT count(*)::bigint AS a, sum(id)::numeric AS b, max(nme)::text AS c FROM cvt_f1;
-- begin-expected
-- columns: d
-- row:  SELECT count(*) AS a,\n    sum(id)::numeric AS b,\n    max(nme) AS c\n   FROM cvt_f1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_fold3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_fold4 AS SELECT (id * 2)::int AS a, (id / 2)::int AS b, (nm + 1)::numeric AS c FROM cvt_f1;
-- begin-expected
-- columns: d
-- row:  SELECT id * 2 AS a,\n    id / 2 AS b,\n    nm + 1::numeric AS c\n   FROM cvt_f1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_fold4'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW cvt_fcase AS SELECT CASE WHEN id = 1 THEN f4 END AS a, CASE WHEN id = 1 THEN vc END AS b, CASE WHEN id = 1 THEN ch END AS c FROM cvt_f1;
-- begin-expected
-- columns: d
-- row:  SELECT\n        CASE\n            WHEN id = 1 THEN f4\n            ELSE NULL::real\n        END AS a,\n        CASE\n            WHEN id = 1 THEN vc\n            ELSE NULL::character varying\n        END AS b,\n        CASE\n            WHEN id = 1 THEN ch\n            ELSE NULL::bpchar\n        END AS c\n   FROM cvt_f1;
-- end-expected
SELECT replace(pg_get_viewdef('cvt_fcase'::regclass, true), chr(10), '\n') AS d;

-- stmt 8: a default and a CHECK read the same constants the same way
CREATE TABLE cvt_e (a int, t time DEFAULT '3:4', tz timetz DEFAULT '3:4+02', arr int[] DEFAULT '{ 1, 2 }', nm numeric[] DEFAULT '{ 01.50 }');
-- begin-expected
-- columns: d
-- row: t='03:04:00'::time without time zone tz='03:04:00+02'::time with time zone arr='{1,2}'::integer[] nm='{1.50}'::numeric[]
-- end-expected
SELECT string_agg(column_name || '=' || column_default, ' ' ORDER BY ordinal_position) AS d FROM information_schema.columns WHERE table_name = 'cvt_e' AND column_default IS NOT NULL;
-- begin-expected
-- columns: d
-- row: '03:04:00'::time without time zone '03:04:00+02'::time with time zone '{1,2}'::integer[] '{1.50}'::numeric[]
-- end-expected
SELECT string_agg(pg_get_expr(x.adbin, x.adrelid), ' ' ORDER BY x.adnum) AS d FROM pg_attrdef x WHERE x.adrelid = 'cvt_e'::regclass;
CREATE TABLE cvt_k (a int, t time, arr int[], CHECK (t > '3:4'), CHECK (arr <> '{ 1, 2 }'));
-- begin-expected
-- columns: d
-- row: CHECK ((arr <> '{1,2}'::integer[]))\nCHECK ((t > '03:04:00'::time without time zone))
-- end-expected
SELECT replace(string_agg(pg_get_constraintdef(oid), chr(10) ORDER BY pg_get_constraintdef(oid)), chr(10), '\n') AS d FROM pg_constraint WHERE conrelid = 'cvt_k'::regclass AND contype = 'c';

-- cleanup
DROP VIEW cvt_w1, cvt_w2, cvt_w3, cvt_w4, cvt_a1, cvt_a2, cvt_a3, cvt_a4, cvt_a5, cvt_a6;
DROP VIEW cvt_ts, cvt_dt, cvt_bool, cvt_val, cvt_net, cvt_bit, cvt_num, cvt_num2, cvt_neg, cvt_cst;
DROP VIEW cvt_fold, cvt_fold2, cvt_fold3, cvt_fold4, cvt_fcase;
DROP TABLE cvt_v1;
DROP TABLE cvt_n1;
DROP TABLE cvt_f1;
DROP TABLE cvt_tm;
DROP TABLE cvt_ar;
DROP TABLE cvt_e;
DROP TABLE cvt_k;
