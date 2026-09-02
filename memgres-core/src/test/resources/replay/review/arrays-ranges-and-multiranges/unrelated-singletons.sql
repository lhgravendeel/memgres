-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Arrays, ranges and multiranges
-- title: Unrelated singletons
-- begin-expected
-- columns: array_to_json:json
-- row: [1,2]
-- rowcount: 1
-- end-expected
SELECT array_to_json(ARRAY[1,2]);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ab' LIKE ANY (ARRAY['a%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ab' NOT LIKE ALL (ARRAY['z%']);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ad (a int[3], b text ARRAY, c text ARRAY[4]);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 1 < 2 IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT ARRAY[1,2] @> ARRAY[1] IS NULL;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ins" does not exist
-- end-expected-error
INSERT INTO zz_vf_ins (i, arr[1]) VALUES (2, 5) RETURNING i, arr;
-- begin-expected
-- columns: sum:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT sum(1) OVER (ORDER BY x RANGE BETWEEN '1 day'::interval PRECEDING AND CURRENT ROW) FROM (VALUES ('1 day'::interval)) t(x);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_g4() RETURNS text AS $$ declare a int[] := array[1,2,3]; begin a[-1] := 9; return a::text; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: lexeme:text | positions:text | weights:text
-- row: a | {1,3} | {A,D}
-- row: b | {2} | {D}
-- rowcount: 2
-- end-expected
SELECT lexeme, positions::text, weights::text FROM unnest('a:1A,3 b:2'::tsvector) ORDER BY lexeme;
-- begin-expected
-- columns: width_bucket:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT width_bucket(5, ARRAY[8,4,1]);
-- begin-expected
-- columns: array_fill:_int4
-- row: {}
-- rowcount: 1
-- end-expected
SELECT array_fill(1, ARRAY[2,0]);
-- begin-expected
-- columns: array_agg:_int4
-- row: {NULL,1,2}
-- rowcount: 1
-- end-expected
SELECT array_agg(v ORDER BY v NULLS FIRST) FROM (VALUES (1),(NULL::int),(2)) t(v);
-- begin-expected
-- columns: array_agg:_int4
-- row: {10,20,30,40,NULL}
-- rowcount: 1
-- end-expected
SELECT array_agg(DISTINCT v) FROM (VALUES (10),(20),(20),(30),(NULL::int),(40)) t(v);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT NULL::int = ANY(ARRAY[]::int[]);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT NULL::int = ALL(ARRAY[]::int[]);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_e" does not exist
-- end-expected-error
SELECT NULL::int IN (SELECT v FROM zz_vf_e);
-- empty table
-- begin-expected
-- columns: daterange:daterange
-- row: ["4713-01-01 BC",2020-01-01)
-- rowcount: 1
-- end-expected
SELECT '[4713-01-01 BC,2020-01-01)'::daterange;
-- begin-expected
-- columns: range_intersect_agg:int4multirange
-- row: {[5,10)}
-- rowcount: 1
-- end-expected
SELECT range_intersect_agg(m) FROM (VALUES ('{[1,10)}'::int4multirange),('{[5,20)}'::int4multirange)) v(m);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_wr" does not exist
-- end-expected-error
SELECT (zz_vf_wr).a FROM zz_vf_wr;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_wr" does not exist
-- end-expected-error
SELECT (zz_vf_wr.*).a FROM zz_vf_wr;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_wr" does not exist
-- end-expected-error
SELECT (zz_vf_wr.*)::text FROM zz_vf_wr;
-- begin-expected
-- columns: dictionary:text | lexemes:text
-- row: simple | {cats}
-- rowcount: 1
-- end-expected
SELECT dictionary::text, lexemes::text FROM ts_debug('simple', 'Cats');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_pt_x" does not exist
-- end-expected-error
SELECT pg_partition_root('zz_vf_pt_x'::regclass)::text;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_pt_x" does not exist
-- end-expected-error
SELECT pg_partition_ancestors('zz_vf_pt_x'::regclass)::text;
-- begin-expected
-- columns: relhassubclass:bool
-- rowcount: 0
-- end-expected
SELECT relhassubclass FROM pg_class WHERE relname='zz_vf_pt';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_sec" does not exist
-- end-expected-error
GRANT UPDATE ON zz_vf_sec TO zz_vf_role WITH GRANT OPTION;
-- begin-expected
-- columns: a:text
-- rowcount: 0
-- end-expected
SELECT a::text FROM (SELECT unnest(relacl) AS a FROM pg_class WHERE relname='zz_vf_sec') s ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_vf_role2" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO zz_vf_role2;
-- begin-expected
-- columns: defaclobjtype:char | defaclacl:text
-- rowcount: 0
-- end-expected
SELECT defaclobjtype, defaclacl::text FROM pg_default_acl d JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='public';
-- begin-expected
-- columns: jsonb_strip_nulls:jsonb
-- row: [1, 2]
-- rowcount: 1
-- end-expected
SELECT jsonb_strip_nulls('[1,null,2]', true);
-- begin-expected
-- columns: json_strip_nulls:json
-- row: [1,null,{}]
-- rowcount: 1
-- end-expected
SELECT json_strip_nulls('[1,null,{"x":null}]');
-- begin-expected
-- columns: jsonb_pretty:text
-- row: {\n}
-- rowcount: 1
-- end-expected
SELECT jsonb_pretty('{}');
