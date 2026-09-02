-- source: review-2026-08.md
-- finding: Root cause 4: arrays are carried as literal text, with a different renderer at every site
-- area: Arrays, ranges and multiranges
-- title: Root cause 4: arrays are carried as literal text, with a different renderer at every site
-- begin-expected
-- columns: array_append:_int4
-- row: [0:2]={1,2,3}
-- rowcount: 1
-- end-expected
SELECT array_append('[0:1]={1,2}'::int[], 3);
-- begin-expected
-- columns: array_remove:_int4
-- row: [0:1]={1,3}
-- rowcount: 1
-- end-expected
SELECT array_remove('[0:2]={1,2,3}'::int[], 2);
-- begin-expected
-- columns: array_to_string:text
-- row: 1,2,3
-- rowcount: 1
-- end-expected
SELECT array_to_string('[0:2]={1,2,3}'::int[], ',');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 1 = ANY('[0:1]={1,2}'::int[]);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '[0:1]={1,2}'::int[] @> '{1}'::int[];
-- begin-expected
-- columns: u:int4
-- row: 1
-- row: 2
-- row: 3
-- rowcount: 3
-- end-expected
SELECT * FROM unnest('[0:2]={1,2,3}'::int[]) AS u;
-- begin-expected
-- columns: generate_subscripts:int4
-- row: -2
-- row: -1
-- row: 0
-- rowcount: 3
-- end-expected
SELECT generate_subscripts('[-2:0]={7,8,9}'::int[], 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_trg() RETURNS trigger AS $$ begin new.seen := TG_ARGV::text; return new; end $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_tg" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_t BEFORE INSERT ON zz_vf_tg FOR EACH ROW EXECUTE FUNCTION zz_vf_trg('a1','a2');
-- begin-expected
-- columns: array:text
-- row: {"NULL"}
-- rowcount: 1
-- end-expected
SELECT ARRAY['NULL']::text;
-- begin-expected
-- columns: timestamp:_timestamp
-- row: {"2020-01-01 10:00:00"}
-- rowcount: 1
-- end-expected
SELECT '{"2020-01-01 10:00:00"}'::timestamp[];
-- begin-expected
-- columns: array_agg:_text
-- row: {"a	b",c}
-- rowcount: 1
-- end-expected
SELECT array_agg(x) FROM (VALUES (E'a\tb'), ('c')) v(x);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_e2 AS ENUM ('a b', 'c''d', 'E', 'e');
-- begin-expected
-- columns: enum_range:_zz_vf_e2
-- row: {"a b",c'd,E,e}
-- rowcount: 1
-- end-expected
SELECT enum_range(NULL::zz_vf_e2);
-- begin-expected
-- columns: regexp_match:text
-- row: 01
-- rowcount: 1
-- end-expected
SELECT (regexp_match('x 01 y', '(\d\d)'))[1];
-- begin-expected
-- columns: array_to_string:text
-- row: 007
-- rowcount: 1
-- end-expected
SELECT array_to_string(regexp_match('x 007 y','(\d+)'),'|');
-- begin-expected
-- columns: array_dims:text | array_dims:text | array_ndims:int4
-- row: [1:1][1:1][1:1] | [1:1][1:1][1:2] | NULL
-- rowcount: 1
-- end-expected
SELECT array_dims('{{{1}}}'::int[]), array_dims(ARRAY[[['a','b']]]), array_ndims('{}'::int[]);
