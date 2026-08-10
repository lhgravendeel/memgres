DROP TABLE IF EXISTS zz_ar CASCADE;

CREATE TABLE zz_ar (id int, a int[], t text[], b bytea[]);

INSERT INTO zz_ar VALUES (1, '{1,2,3}', '{a,b}', ARRAY['\x0102'::bytea]);

-- begin-expected
-- columns: array
-- row: {"a\\b"}
-- end-expected
SELECT ARRAY['a\b']::text;

-- begin-expected
-- columns: array
-- row: {"a\"b"}
-- end-expected
SELECT ARRAY['a"b']::text;

-- begin-expected
-- columns: array
-- row: {"a,b"}
-- end-expected
SELECT ARRAY['a,b']::text;

-- begin-expected
-- columns: array
-- row: {"a b"}
-- end-expected
SELECT ARRAY['a b']::text;

-- begin-expected
-- columns: array
-- row: {""}
-- end-expected
SELECT ARRAY['']::text;

-- begin-expected
-- columns: array
-- row: {"NULL"}
-- end-expected
SELECT ARRAY['NULL']::text;

-- begin-expected
-- columns: array
-- row: {NULL}
-- end-expected
SELECT ARRAY[NULL::text]::text;

-- begin-expected
-- columns: array
-- row: {"{a}"}
-- end-expected
SELECT ARRAY['{a}']::text;

-- begin-expected
-- columns: array
-- row: {"a	b"}
-- end-expected
SELECT ARRAY[E'a\tb']::text;

-- begin-expected
-- columns: array
-- row: {a'b}
-- end-expected
SELECT ARRAY['a''b']::text;

-- begin-expected
-- columns: array
-- row: {"  a  "}
-- end-expected
SELECT ARRAY['  a  ']::text;

-- begin-expected
-- columns: array
-- row: {t,f}
-- end-expected
SELECT ARRAY[true,false]::text;

-- begin-expected
-- columns: array
-- row: {"\\x0102"}
-- end-expected
SELECT ARRAY['\x0102'::bytea]::text;

-- begin-expected
-- columns: array
-- row: {0.0000000000000001}
-- end-expected
SELECT ARRAY['1e-16'::numeric]::text;

-- begin-expected
-- columns: array
-- row: {1.50}
-- end-expected
SELECT ARRAY[1.50::numeric]::text;

-- begin-expected
-- columns: array
-- row: {"2020-01-01 10:00:00"}
-- end-expected
SELECT ARRAY['2020-01-01 10:00:00'::timestamp]::text;

-- begin-expected
-- columns: array
-- row: {"4713-01-01 BC"}
-- end-expected
SELECT ARRAY['4713-01-01 BC'::date]::text;

-- begin-expected
-- columns: array
-- row: {"1 day"}
-- end-expected
SELECT ARRAY['1 day'::interval]::text;

-- begin-expected
-- columns: array
-- row: {192.168.0.1}
-- end-expected
SELECT ARRAY['192.168.0.1'::inet]::text;

-- begin-expected
-- columns: array_to_string
-- row: t,f
-- end-expected
SELECT array_to_string(ARRAY[true,false,NULL], ',');

-- begin-expected
-- columns: array_to_string
-- row: t,f,X
-- end-expected
SELECT array_to_string(ARRAY[true,false,NULL], ',', 'X');

-- begin-expected
-- columns: array_to_string
-- row: 1-2-3-4
-- end-expected
SELECT array_to_string(ARRAY[[1,2],[3,4]], '-');

-- begin-expected
-- columns: text
-- row: [0:1]={1,2}
-- end-expected
SELECT '[0:1]={1,2}'::int[]::text;

-- begin-expected
-- columns: array_lower|array_upper
-- row: 0|1
-- end-expected
SELECT array_lower('[0:1]={1,2}'::int[],1), array_upper('[0:1]={1,2}'::int[],1);

-- begin-expected
-- columns: array_append
-- row: [0:2]={1,2,3}
-- end-expected
SELECT array_append('[0:1]={1,2}'::int[], 3)::text;

-- begin-expected
-- columns: array_prepend
-- row: [0:2]={0,1,2}
-- end-expected
SELECT array_prepend(0, '[0:1]={1,2}'::int[])::text;

-- begin-expected
-- columns: array_cat
-- row: [3:6]={1,2,5,6}
-- end-expected
SELECT array_cat('[3:4]={1,2}'::int[], '[7:8]={5,6}'::int[])::text;

-- begin-expected
-- columns: array_remove
-- row: [0:1]={1,3}
-- end-expected
SELECT array_remove('[0:2]={1,2,3}'::int[], 2)::text;

-- begin-expected
-- columns: array_replace
-- row: [0:2]={1,9,3}
-- end-expected
SELECT array_replace('[0:2]={1,2,3}'::int[], 2, 9)::text;

-- begin-expected
-- columns: array_position
-- row: 2
-- end-expected
SELECT array_position('[0:2]={1,2,3}'::int[], 3);

-- begin-expected
-- columns: array_positions
-- row: {2}
-- end-expected
SELECT array_positions('[0:2]={1,2,3}'::int[], 3)::text;

-- begin-expected
-- columns: array_to_string
-- row: 1,2,3
-- end-expected
SELECT array_to_string('[0:2]={1,2,3}'::int[], ',');

-- begin-expected
-- columns: unnest
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT unnest('[0:2]={1,2,3}'::int[]);

-- begin-expected
-- columns: generate_subscripts
-- row: -2
-- row: -1
-- row: 0
-- end-expected
SELECT generate_subscripts('[-2:0]={7,8,9}'::int[], 1);

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT 1 = ANY('[0:1]={1,2}'::int[]);

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '[0:1]={1,2}'::int[] @> '{1}'::int[];

-- begin-expected
-- columns: cardinality
-- row: 2
-- end-expected
SELECT cardinality('[0:1]={1,2}'::int[]);

-- begin-expected
-- columns: array_dims
-- row: [0:1]
-- end-expected
SELECT array_dims('[0:1]={1,2}'::int[]);

-- begin-expected
-- columns: array_ndims|array_dims|array_length
-- row: NULL|NULL|NULL
-- end-expected
SELECT array_ndims('{}'::int[]), array_dims('{}'::int[]), array_length('{}'::int[],1);

-- begin-expected
-- columns: array_dims
-- row: [1:1][1:1][1:1]
-- end-expected
SELECT array_dims('{{{1}}}'::int[]);

-- begin-expected
-- columns: array_dims
-- row: [1:1][1:1][1:2]
-- end-expected
SELECT array_dims(ARRAY[[['a','b']]]);

-- begin-expected
-- columns: array_fill
-- row: [3:4]={1,1}
-- end-expected
SELECT array_fill(1, ARRAY[2], ARRAY[3])::text;

-- begin-expected
-- columns: array_fill
-- row: {}
-- end-expected
SELECT array_fill(1, ARRAY[2,0])::text;

-- begin-expected
-- columns: trim_array
-- row: {1,2}
-- end-expected
SELECT trim_array('[0:2]={1,2,3}'::int[], 1)::text;

-- begin-expected
-- columns: array_replace
-- row: {{1,2},{9,4}}
-- end-expected
SELECT array_replace(ARRAY[[1,2],[3,4]], 3, 9)::text;

-- begin-expected
-- columns: array_to_json
-- row: [1,2]
-- end-expected
SELECT array_to_json(ARRAY[1,2]);

-- begin-expected
-- columns: array_to_json
-- row: [[1,2],[3,4]]
-- end-expected
SELECT array_to_json(ARRAY[[1,2],[3,4]]);

-- begin-expected
-- columns: width_bucket|width_bucket|width_bucket
-- row: 3|0|3
-- end-expected
SELECT width_bucket(5, ARRAY[8,4,1]), width_bucket(0, ARRAY[8,4,1]), width_bucket(9, ARRAY[8,4,1]);

-- begin-expected
-- columns: width_bucket|width_bucket|width_bucket
-- row: 2|0|3
-- end-expected
SELECT width_bucket(5, ARRAY[1,4,8]), width_bucket(0, ARRAY[1,4,8]), width_bucket(9, ARRAY[1,4,8]);

-- begin-expected
-- columns: array_agg
-- row: {"a	b",c}
-- end-expected
SELECT array_agg(x ORDER BY x)::text FROM (VALUES (E'a\tb'), ('c')) v(x);

-- begin-expected
-- columns: array_agg
-- row: {NULL,1,2}
-- end-expected
SELECT array_agg(v ORDER BY v NULLS FIRST)::text FROM (VALUES (1),(NULL::int),(2)) t(v);

-- begin-expected
-- columns: array_agg
-- row: {10,20,30,40,NULL}
-- end-expected
SELECT array_agg(DISTINCT v)::text FROM (VALUES (10),(20),(20),(30),(NULL::int),(40)) t(v);

-- begin-expected
-- columns: array_agg
-- row: {t}
-- end-expected
SELECT array_agg(x)::text FROM (SELECT true AS x) t;

-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT NULL::int = ANY(ARRAY[]::int[]);

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT NULL::int = ALL(ARRAY[]::int[]);

-- begin-expected
-- columns: a
-- row: {1,2,3}
-- end-expected
SELECT a::text FROM zz_ar;

-- begin-expected
-- columns: b
-- row: {"\\x0102"}
-- end-expected
SELECT b::text FROM zz_ar;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT b[1] = '\x0102'::bytea AS ok FROM zz_ar;

-- begin-expected
-- columns: u
-- row: \x0102
-- end-expected
SELECT unnest(b)::text AS u FROM zz_ar;

-- begin-expected
-- columns: ?column?
-- row: {1,2,NULL}
-- end-expected
SELECT ARRAY[1,2] || NULL::int;

-- begin-expected
-- columns: cardinality
-- row: 3
-- end-expected
SELECT cardinality(ARRAY[1,2] || NULL::int);

-- begin-expected
-- columns: ?column?
-- row: {a,b}c
-- end-expected
SELECT '{a,b}' || 'c';

-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT 'a' IN ('{a,b}');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type integer: "3.7"
-- end-expected-error
SELECT array_append(ARRAY[1,2], '3.7');

-- begin-expected-error
-- sqlstate: 22000
-- message-like: ERROR: argument must be empty or one-dimensional array
-- end-expected-error
SELECT array_append(ARRAY[[1,2],[3,4]], 5);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: searching for elements in multidimensional arrays is not supported
-- end-expected-error
SELECT array_position(ARRAY[[1,2],[3,4]], 3);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: removing elements from multidimensional arrays is not supported
-- end-expected-error
SELECT array_remove(ARRAY[[1,2],[3,4]], 3);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: sample size must be between 0 and 3
-- end-expected-error
SELECT array_sample(ARRAY[1,2,3], 5);

-- begin-expected-error
-- sqlstate: 2202E
-- message-like: ERROR: wrong number of array subscripts
-- end-expected-error
SELECT array_fill(1, ARRAY[[2]]);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: function array_length(text, integer) does not exist
-- end-expected-error
SELECT array_length('{1,2,3}'::text, 1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: function unnest(text) does not exist
-- end-expected-error
SELECT unnest('{1,2,3}'::text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: ERROR: collations are not supported by type integer[]
-- end-expected-error
SELECT ('{1,2}'::int[] COLLATE "C");

DROP TABLE zz_ar;

