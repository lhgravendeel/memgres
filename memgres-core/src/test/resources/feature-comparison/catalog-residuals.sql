-- Catalog residuals: rows that described something other than what the engine does.
--  1. pg_cast pairs each range type with its own multirange, and the multirange types
--     carry PostgreSQL's OIDs, so pg_type, pg_range and pg_cast agree about what a
--     range converts to.
--  2. The aclitem, oidvector and jsonpath operators are in pg_operator, oidvector is a
--     type a cast can name, and jsonb @? / @@ answer instead of always saying false.
--  3. int2vector and oidvector are subscripted from zero, so array_length over an empty
--     one is 0 and its bounds are [0:-1] — the shape PostgreSQL's own opr_sanity reads.
--  4. The core btree operator classes and families are all present.
--  5. The builtin-provider collations state their locale in colllocale and leave the
--     libc columns null.
--  6. pg_get_function_arguments and pg_get_function_result render from the pg_proc row.
--  7. MERGE ... WHEN NOT MATCHED BY SOURCE reaches only the target rows no source row
--     matched, whether or not a WHEN MATCHED arm is present.
--  8. pg_get_viewdef parenthesises every operator; its second argument prunes the pairs
--     precedence makes unnecessary, and a wrap column keeps the select list on one line.
--     Newlines are folded to '~' so a definition fits one annotation line.

-- stmt 1: every range type names its own multirange
-- begin-expected
-- columns: src | tgt | ctx | meth
-- row: daterange | datemultirange | e | f
-- row: int4range | int4multirange | e | f
-- row: int8range | int8multirange | e | f
-- row: numrange | nummultirange | e | f
-- row: tsrange | tsmultirange | e | f
-- row: tstzrange | tstzmultirange | e | f
-- end-expected
SELECT s.typname::text AS src, t.typname::text AS tgt, c.castcontext::text AS ctx, c.castmethod::text AS meth FROM pg_cast c JOIN pg_type s ON s.oid = c.castsource JOIN pg_type t ON t.oid = c.casttarget WHERE s.typname::text IN ('int4range','int8range','numrange','tsrange','tstzrange','daterange') ORDER BY 1, 2;

-- stmt 2: the multirange types carry PostgreSQL's own OIDs
-- begin-expected
-- columns: nm | id
-- row: datemultirange | 4535
-- row: int4multirange | 4451
-- row: int8multirange | 4536
-- row: nummultirange | 4532
-- row: tsmultirange | 4533
-- row: tstzmultirange | 4534
-- end-expected
SELECT typname::text AS nm, oid::int AS id FROM pg_type WHERE typname::text IN ('int4multirange','int8multirange','nummultirange','tsmultirange','tstzmultirange','datemultirange') ORDER BY 1;

-- stmt 3: and so do their array types, each pointing back at its element
-- begin-expected
-- columns: nm | id | elem
-- row: _datemultirange | 6155 | 4535
-- row: _int4multirange | 6150 | 4451
-- row: _int8multirange | 6157 | 4536
-- row: _nummultirange | 6151 | 4532
-- row: _tsmultirange | 6152 | 4533
-- row: _tstzmultirange | 6153 | 4534
-- end-expected
SELECT typname::text AS nm, oid::int AS id, typelem::int AS elem FROM pg_type WHERE typname::text IN ('_int4multirange','_int8multirange','_nummultirange','_tsmultirange','_tstzmultirange','_datemultirange') ORDER BY 1;

-- stmt 4: pg_range agrees with pg_cast about the pairing
-- begin-expected
-- columns: rng | sub | multi
-- row: daterange | date | datemultirange
-- row: int4range | integer | int4multirange
-- row: int8range | bigint | int8multirange
-- row: numrange | numeric | nummultirange
-- row: tsrange | timestamp without time zone | tsmultirange
-- row: tstzrange | timestamp with time zone | tstzmultirange
-- end-expected
SELECT rngtypid::regtype::text AS rng, rngsubtype::regtype::text AS sub, rngmultitypid::regtype::text AS multi FROM pg_range WHERE rngtypid::regtype::text IN ('int4range','int8range','numrange','tsrange','tstzrange','daterange') ORDER BY 1;

-- stmt 5: the conversions themselves
-- begin-expected
-- columns: a | b | c | d
-- row: {[1,2)} | {[2020-01-01,2020-02-01)} | {[1,5)} | {[1,5)}
-- end-expected
SELECT numrange(1,2)::nummultirange::text AS a, daterange('2020-01-01','2020-02-01')::datemultirange::text AS b, int8range(1,5)::int8multirange::text AS c, int4range(1,5)::int4multirange::text AS d;

-- stmt 6: the jsonpath operators answer rather than always saying false
-- begin-expected
-- columns: a | b | c | d
-- row: t | t | f | f
-- end-expected
SELECT '{"a":1}'::jsonb @? '$.a'::jsonpath AS a, '{"a":1}'::jsonb @@ '$.a == 1'::jsonpath AS b, '{"a":1}'::jsonb @? '$.b'::jsonpath AS c, '{"a":1}'::jsonb @@ '$.a == 2'::jsonpath AS d;

-- stmt 7: a nested key, and the quoted spelling PostgreSQL's jsonpath prints
-- begin-expected
-- columns: a | b
-- row: t | t
-- end-expected
SELECT '{"a":{"b":2}}'::jsonb @? '$.a.b'::jsonpath AS a, '{"a":{"b":2}}'::jsonb @? '$."a"."b"'::jsonpath AS b;

-- stmt 8: oidvector is a type a cast can name, and it compares
-- begin-expected
-- columns: t | a | b | c | d | e | f
-- row: 1 2 | t | t | t | t | t | t
-- end-expected
SELECT '1 2'::oidvector::text AS t, '1 2'::oidvector = '1 2'::oidvector AS a, '1 2'::oidvector < '1 3'::oidvector AS b, '1 2'::oidvector <> '1 3'::oidvector AS c, '1 2'::oidvector <= '1 2'::oidvector AS d, '2 2'::oidvector > '1 3'::oidvector AS e, '1 2'::oidvector >= '1 2'::oidvector AS f;

-- stmt 9: both vector types resolve as regtype
-- begin-expected
-- columns: a | b
-- row: oidvector | int2vector
-- end-expected
SELECT 'oidvector'::regtype::text AS a, 'int2vector'::regtype::text AS b;

-- stmt 10: the aclitem, oidvector and jsonpath operator rows
-- begin-expected
-- columns: nm | l | r | res
-- row: + | aclitem[] | aclitem | aclitem[]
-- row: - | aclitem[] | aclitem | aclitem[]
-- row: < | oidvector | oidvector | boolean
-- row: <= | oidvector | oidvector | boolean
-- row: <> | oidvector | oidvector | boolean
-- row: = | aclitem | aclitem | boolean
-- row: = | oidvector | oidvector | boolean
-- row: > | oidvector | oidvector | boolean
-- row: >= | oidvector | oidvector | boolean
-- row: @> | aclitem[] | aclitem | boolean
-- row: @? | jsonb | jsonpath | boolean
-- row: @@ | jsonb | jsonpath | boolean
-- end-expected
SELECT o.oprname::text AS nm, o.oprleft::regtype::text AS l, o.oprright::regtype::text AS r, o.oprresult::regtype::text AS res FROM pg_operator o WHERE o.oprleft::regtype::text IN ('aclitem[]','aclitem','oidvector') OR o.oprright::regtype::text = 'jsonpath' ORDER BY 1, 2, 3;

-- stmt 11: no operator names a function with no pg_proc row
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_operator o WHERE NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = o.oprcode);

-- stmt 12: an empty oidvector still has a dimension, and it runs from zero
-- begin-expected
-- columns: nargs | len | lo | hi | card
-- row: 0 | 0 | 0 | -1 | 0
-- end-expected
SELECT pronargs::int AS nargs, array_length(proargtypes,1) AS len, array_lower(proargtypes,1) AS lo, array_upper(proargtypes,1) AS hi, cardinality(proargtypes)::int AS card FROM pg_proc WHERE proname = 'now';

-- stmt 13: a one-argument function's vector runs 0..0
-- begin-expected
-- columns: nargs | len | lo | hi
-- row: 1 | 1 | 0 | 0
-- end-expected
SELECT DISTINCT pronargs::int AS nargs, array_length(proargtypes,1) AS len, array_lower(proargtypes,1) AS lo, array_upper(proargtypes,1) AS hi FROM pg_proc WHERE proname = 'abs';

-- stmt 14: PostgreSQL's own opr_sanity check finds the zero-argument functions
-- begin-expected
-- columns: any
-- row: t
-- end-expected
SELECT count(*)::int > 0 AS any FROM pg_proc WHERE array_length(proargtypes,1) IS DISTINCT FROM NULLIF(pronargs,0);

-- stmt 15: an ordinary empty array still has no dimensions at all
-- begin-expected
-- columns: a | b | c | d | e
-- row: 3 | NULL | 1 | 2 | 0
-- end-expected
SELECT array_length(ARRAY[1,2,3],1) AS a, array_length(ARRAY[]::int[],1) AS b, array_lower(ARRAY[1,2],1) AS c, array_upper(ARRAY[1,2],1) AS d, cardinality(ARRAY[]::int[])::int AS e;

-- stmt 16: the core btree operator classes
-- begin-expected
-- columns: nm | def
-- row: array_ops | t
-- row: bool_ops | t
-- row: bpchar_ops | t
-- row: bytea_ops | t
-- row: date_ops | t
-- row: float8_ops | t
-- row: inet_ops | t
-- row: int4_ops | t
-- row: int8_ops | t
-- row: interval_ops | t
-- row: jsonb_ops | t
-- row: money_ops | t
-- row: name_ops | t
-- row: numeric_ops | t
-- row: oid_ops | t
-- row: text_ops | t
-- row: time_ops | t
-- row: timestamp_ops | t
-- row: uuid_ops | t
-- row: varchar_ops | f
-- end-expected
SELECT o.opcname::text AS nm, o.opcdefault AS def FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod WHERE a.amname::text = 'btree' AND o.opcname::text IN ('int4_ops','int8_ops','text_ops','varchar_ops','bool_ops','date_ops','timestamp_ops','uuid_ops','numeric_ops','bytea_ops','float8_ops','interval_ops','array_ops','bpchar_ops','oid_ops','name_ops','jsonb_ops','inet_ops','time_ops','money_ops') ORDER BY 1;

-- stmt 17: the families those classes belong to
-- begin-expected
-- columns: nm
-- row: array_ops
-- row: bool_ops
-- row: bytea_ops
-- row: datetime_ops
-- row: float_ops
-- row: integer_ops
-- row: interval_ops
-- row: jsonb_ops
-- row: money_ops
-- row: network_ops
-- row: numeric_ops
-- row: oid_ops
-- row: text_ops
-- row: time_ops
-- row: uuid_ops
-- end-expected
SELECT f.opfname::text AS nm FROM pg_opfamily f JOIN pg_am a ON a.oid = f.opfmethod WHERE a.amname::text = 'btree' AND f.opfname::text IN ('integer_ops','text_ops','datetime_ops','numeric_ops','bool_ops','uuid_ops','array_ops','bytea_ops','float_ops','interval_ops','network_ops','jsonb_ops','time_ops','money_ops','oid_ops') ORDER BY 1;

-- stmt 18: a class names the type it indexes and the family it belongs to
-- begin-expected
-- columns: nm | intype | def | fam
-- row: array_ops | anyarray | t | array_ops
-- row: inet_ops | inet | t | network_ops
-- row: name_ops | name | t | text_ops
-- row: oidvector_ops | oidvector | t | oidvector_ops
-- row: varchar_ops | text | f | text_ops
-- row: varchar_pattern_ops | text | f | text_pattern_ops
-- end-expected
SELECT o.opcname::text AS nm, o.opcintype::regtype::text AS intype, o.opcdefault AS def, f.opfname::text AS fam FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod JOIN pg_opfamily f ON f.oid = o.opcfamily WHERE a.amname::text = 'btree' AND o.opcname::text IN ('array_ops','inet_ops','name_ops','oidvector_ops','varchar_ops','varchar_pattern_ops') ORDER BY 1;

-- stmt 19: no operator class points at a type, family or method that is not there
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_opclass o WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.opcintype) OR NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = o.opcfamily) OR NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = o.opcmethod);

-- stmt 20: no two operator classes share an OID
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM (SELECT oid FROM pg_opclass GROUP BY oid HAVING count(*) > 1) d;

-- stmt 21: the builtin collations state their locale, not the libc columns
-- begin-expected
-- columns: nm | prov | det | coll | ctype | loc
-- row: C | c | t | C | C | NULL
-- row: POSIX | c | t | POSIX | POSIX | NULL
-- row: default | d | t | NULL | NULL | NULL
-- row: pg_c_utf8 | b | t | NULL | NULL | C.UTF-8
-- row: ucs_basic | b | t | NULL | NULL | C
-- row: unicode | i | t | NULL | NULL | und
-- end-expected
SELECT collname::text AS nm, collprovider::text AS prov, collisdeterministic AS det, collcollate AS coll, collctype AS ctype, colllocale AS loc FROM pg_collation WHERE collname::text IN ('default','C','POSIX','ucs_basic','unicode','pg_c_utf8') ORDER BY 1;

-- stmt 22: each of those names is one COLLATE accepts
-- begin-expected
-- columns: a | b | c | d
-- row: t | t | t | t
-- end-expected
SELECT 'b' COLLATE "ucs_basic" < 'c' COLLATE "ucs_basic" AS a, 'b' COLLATE "unicode" < 'c' COLLATE "unicode" AS b, 'b' COLLATE "pg_c_utf8" < 'c' COLLATE "pg_c_utf8" AS c, 'b' COLLATE "C" < 'c' COLLATE "C" AS d;

-- stmt 23: a name no collation has is still refused
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "cr2_nope" for encoding "UTF8" does not exist
-- end-expected-error
SELECT 'b' COLLATE "cr2_nope";

-- stmt 24: a function's argument list renders from its pg_proc row
-- begin-expected
-- columns: args
-- row: bigint
-- row: double precision
-- row: integer
-- row: numeric
-- row: real
-- row: smallint
-- end-expected
SELECT DISTINCT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'abs' ORDER BY 1;

-- stmt 25: and so does its result
-- begin-expected
-- columns: res
-- row: bigint
-- row: double precision
-- row: integer
-- row: numeric
-- row: real
-- row: smallint
-- end-expected
SELECT DISTINCT pg_get_function_result(oid) AS res FROM pg_proc WHERE proname = 'abs' ORDER BY 1;

-- stmt 26: a polymorphic overload names the polymorphic type
-- begin-expected
-- columns: args | res
-- row: anymultirange | anyelement
-- row: anyrange | anyelement
-- row: text | text
-- end-expected
SELECT pg_get_function_arguments(oid) AS args, pg_get_function_result(oid) AS res FROM pg_proc WHERE proname = 'upper' ORDER BY 1;

-- stmt 27: a function taking nothing renders an empty argument list
-- begin-expected
-- columns: args | ident | res
-- row: NULL | NULL | timestamp with time zone
-- end-expected
SELECT nullif(pg_get_function_arguments(oid), '') AS args, nullif(pg_get_function_identity_arguments(oid), '') AS ident, pg_get_function_result(oid) AS res FROM pg_proc WHERE proname = 'now';

-- stmt 28: jsonb_set's result comes off the same row
-- begin-expected
-- columns: res
-- row: jsonb
-- end-expected
SELECT pg_get_function_result(oid) AS res FROM pg_proc WHERE proname = 'jsonb_set';

-- setup for the MERGE checks
CREATE TABLE cr2_mt (id int PRIMARY KEY, v int);
CREATE TABLE cr2_ms (id int PRIMARY KEY, v int);
INSERT INTO cr2_mt VALUES (1,10),(2,20),(3,30);
INSERT INTO cr2_ms VALUES (2,200),(3,300),(4,400);

-- stmt 29: WHEN NOT MATCHED BY SOURCE with no WHEN MATCHED arm touches only the
-- target rows no source row matched
MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- begin-expected
-- columns: id | v
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT id, v FROM cr2_mt ORDER BY id;

-- stmt 30: the same for an UPDATE arm
DELETE FROM cr2_mt;
INSERT INTO cr2_mt VALUES (1,10),(2,20),(3,30);
MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = -1;

-- begin-expected
-- columns: id | v
-- row: 1 | -1
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT id, v FROM cr2_mt ORDER BY id;

-- stmt 31: an AND condition narrows the unmatched rows rather than replacing the test
CREATE TABLE cr2_ct (id int PRIMARY KEY, v int);
CREATE TABLE cr2_cs (id int PRIMARY KEY, v int);
INSERT INTO cr2_ct VALUES (1,10),(2,20),(3,30),(5,50);
INSERT INTO cr2_cs VALUES (2,200),(3,300);
MERGE INTO cr2_ct t USING cr2_cs s ON t.id = s.id WHEN NOT MATCHED BY SOURCE AND t.v > 20 THEN DELETE;

-- begin-expected
-- columns: id | v
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT id, v FROM cr2_ct ORDER BY id;

-- stmt 32: the arm keeps working beside the other two
DELETE FROM cr2_mt;
INSERT INTO cr2_mt VALUES (1,10),(2,20),(3,30);
MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v) WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- begin-expected
-- columns: id | v
-- row: 2 | 200
-- row: 3 | 300
-- row: 4 | 400
-- end-expected
SELECT id, v FROM cr2_mt ORDER BY id;

-- stmt 33: an ON condition that is not an equality is read the same way
DELETE FROM cr2_mt;
INSERT INTO cr2_mt VALUES (1,10),(2,20),(3,30);
DELETE FROM cr2_ms;
INSERT INTO cr2_ms VALUES (2,200);
MERGE INTO cr2_mt t USING cr2_ms s ON t.id < s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 0;

-- begin-expected
-- columns: id | v
-- row: 1 | 10
-- row: 2 | 0
-- row: 3 | 0
-- end-expected
SELECT id, v FROM cr2_mt ORDER BY id;

-- stmt 34: a WHEN clause after an unconditional one of its own kind is unreachable
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id WHEN MATCHED THEN DELETE WHEN MATCHED AND t.v > 0 THEN DELETE;

-- stmt 35: the same for two unconditional WHEN NOT MATCHED clauses
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v) WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);

-- stmt 36: and for WHEN NOT MATCHED BY SOURCE
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE WHEN NOT MATCHED BY SOURCE AND t.v > 0 THEN DELETE;

-- stmt 37: a clause of another kind after an unconditional one is reachable
CREATE TABLE cr2_rt (id int PRIMARY KEY, v int);
CREATE TABLE cr2_rs (id int PRIMARY KEY, v int);
INSERT INTO cr2_rt VALUES (1,10);
INSERT INTO cr2_rs VALUES (1,100),(2,200);
MERGE INTO cr2_rt t USING cr2_rs s ON t.id = s.id WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);

-- begin-expected
-- columns: id | v
-- row: 2 | 200
-- end-expected
SELECT id, v FROM cr2_rt ORDER BY id;

-- stmt 38: a conditional arm before an unconditional one is the ordinary shape
DELETE FROM cr2_rt;
INSERT INTO cr2_rt VALUES (1,10),(2,20);
MERGE INTO cr2_rt t USING cr2_rs s ON t.id = s.id WHEN MATCHED AND t.v > 15 THEN DELETE WHEN MATCHED THEN UPDATE SET v = 0;

-- begin-expected
-- columns: id | v
-- row: 1 | 0
-- end-expected
SELECT id, v FROM cr2_rt ORDER BY id;

-- stmt 39: a statement that simply runs out is reported at end of input
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id;

-- stmt 40: so is a SELECT that stops after FROM
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT * FROM;

-- stmt 41: a statement that stops on a word is still reported against that word
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FROM"
-- end-expected-error
SELECT FROM FROM;

-- stmt 42: MERGE onto a view it cannot write through is refused by its own action
CREATE TABLE cr2_vt (id int PRIMARY KEY, v int);
CREATE VIEW cr2_vd AS SELECT DISTINCT id, v FROM cr2_vt;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot update view "cr2_vd"
-- end-expected-error
MERGE INTO cr2_vd t USING cr2_ms s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v;

-- stmt 43: an INSERT arm is refused as an insert
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "cr2_vd"
-- end-expected-error
MERGE INTO cr2_vd t USING cr2_ms s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);

-- stmt 44: pg_get_viewdef parenthesises every operator once
CREATE TABLE cr2_dt (a int PRIMARY KEY, b text);
CREATE VIEW cr2_dv AS SELECT a, b FROM cr2_dt WHERE a > 1;

-- begin-expected
-- columns: def
-- row: SELECT a,~    b~   FROM cr2_dt~  WHERE (a > 1);
-- end-expected
SELECT replace(pg_get_viewdef('cr2_dv'::regclass), chr(10), '~') AS def;

-- stmt 45: pg_views.definition uses the same form
-- begin-expected
-- columns: def
-- row: SELECT a,~    b~   FROM cr2_dt~  WHERE (a > 1);
-- end-expected
SELECT replace(definition, chr(10), '~') AS def FROM pg_views WHERE viewname = 'cr2_dv';

-- stmt 46: the second argument prunes the pairs precedence makes unnecessary
-- begin-expected
-- columns: def
-- row: SELECT a,~    b~   FROM cr2_dt~  WHERE a > 1;
-- end-expected
SELECT replace(pg_get_viewdef('cr2_dv'::regclass, true), chr(10), '~') AS def;

-- stmt 47: a wrap column keeps the select list on one line when it fits
-- begin-expected
-- columns: def
-- row: SELECT a, b~   FROM cr2_dt~  WHERE a > 1;
-- end-expected
SELECT replace(pg_get_viewdef('cr2_dv'::regclass, 80), chr(10), '~') AS def;

-- stmt 48: OR keeps its pair inside an AND, because AND binds tighter
CREATE TABLE cr2_pt (a int PRIMARY KEY, b int, c int);
CREATE VIEW cr2_ov AS SELECT a FROM cr2_pt WHERE a > 1 OR b < 2;
CREATE VIEW cr2_av AS SELECT a FROM cr2_pt WHERE (a > 1 OR b < 2) AND c = 3;

-- begin-expected
-- columns: def
-- row: SELECT a~   FROM cr2_pt~  WHERE ((a > 1) OR (b < 2));
-- end-expected
SELECT replace(pg_get_viewdef('cr2_ov'::regclass), chr(10), '~') AS def;

-- stmt 49: with the pairs pruned there are none left at the top
-- begin-expected
-- columns: def
-- row: SELECT a~   FROM cr2_pt~  WHERE a > 1 OR b < 2;
-- end-expected
SELECT replace(pg_get_viewdef('cr2_ov'::regclass, true), chr(10), '~') AS def;

-- stmt 50: the nested OR keeps exactly one pair
-- begin-expected
-- columns: def
-- row: SELECT a~   FROM cr2_pt~  WHERE (a > 1 OR b < 2) AND c = 3;
-- end-expected
SELECT replace(pg_get_viewdef('cr2_av'::regclass, true), chr(10), '~') AS def;

-- stmt 51: arithmetic keeps none of them
CREATE VIEW cr2_arv AS SELECT a * 2 + b AS x, a - b AS y FROM cr2_pt;

-- begin-expected
-- columns: def
-- row: SELECT a * 2 + b AS x,~    a - b AS y~   FROM cr2_pt;
-- end-expected
SELECT replace(pg_get_viewdef('cr2_arv'::regclass, true), chr(10), '~') AS def;

-- stmt 52: every clause starts its own line
CREATE TABLE cr2_gt (a int PRIMARY KEY, b int);
CREATE VIEW cr2_gv AS SELECT b, count(*) AS n FROM cr2_gt GROUP BY b HAVING count(*) > 1;

-- begin-expected
-- columns: def
-- row: SELECT b,~    count(*) AS n~   FROM cr2_gt~  GROUP BY b~ HAVING (count(*) > 1);
-- end-expected
SELECT replace(pg_get_viewdef('cr2_gv'::regclass), chr(10), '~') AS def;

-- stmt 53: and the pruned form breaks the same way
-- begin-expected
-- columns: def
-- row: SELECT b,~    count(*) AS n~   FROM cr2_gt~  GROUP BY b~ HAVING count(*) > 1;
-- end-expected
SELECT replace(pg_get_viewdef('cr2_gv'::regclass, true), chr(10), '~') AS def;

-- stmt 54: an IN list is deparsed as the scalar-array comparison it is
CREATE TABLE cr2_it (a int PRIMARY KEY, b int);
CREATE VIEW cr2_iv AS SELECT a FROM cr2_it WHERE a IN (1,2);
CREATE VIEW cr2_niv AS SELECT a FROM cr2_it WHERE a NOT IN (1,2);

-- begin-expected
-- columns: def
-- row: SELECT a~   FROM cr2_it~  WHERE (a = ANY (ARRAY[1, 2]));
-- end-expected
SELECT replace(pg_get_viewdef('cr2_iv'::regclass), chr(10), '~') AS def;

-- stmt 55: NOT IN is the same comparison negated
-- begin-expected
-- columns: def
-- row: SELECT a~   FROM cr2_it~  WHERE (a <> ALL (ARRAY[1, 2]));
-- end-expected
SELECT replace(pg_get_viewdef('cr2_niv'::regclass), chr(10), '~') AS def;

-- cleanup
DROP VIEW cr2_niv;
DROP VIEW cr2_iv;
DROP TABLE cr2_it;
DROP VIEW cr2_gv;
DROP TABLE cr2_gt;
DROP VIEW cr2_arv;
DROP VIEW cr2_av;
DROP VIEW cr2_ov;
DROP TABLE cr2_pt;
DROP VIEW cr2_dv;
DROP TABLE cr2_dt;
DROP VIEW cr2_vd;
DROP TABLE cr2_vt;
DROP TABLE cr2_rt;
DROP TABLE cr2_rs;
DROP TABLE cr2_ct;
DROP TABLE cr2_cs;
DROP TABLE cr2_mt;
DROP TABLE cr2_ms;
