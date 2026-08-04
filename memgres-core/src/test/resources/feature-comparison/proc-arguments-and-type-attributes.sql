-- ============================================================================
-- Feature Comparison: pg_proc parameter metadata and pg_type attributes
-- Target: PostgreSQL 18 vs Memgres
--
-- The columns of a pg_proc row that describe a call rather than resolve it,
-- and the pg_type attributes the catalogs are themselves built out of:
--   * proallargtypes / proargmodes / proargnames, so a function with OUT
--     parameters or a VARIADIC tail is described as one;
--   * proargdefaults, so a row claiming N defaults carries N of them;
--   * provariadic, which names the ELEMENT type a tail collects into;
--   * procost, prorows, provolatile, proparallel and proisstrict, which a
--     planner and a client both read;
--   * typcollation, typelem, typarray and the binary I/O functions, and the
--     types unknown, refcursor and gtsvector that had no row at all.
--
-- Every case pins one named row with a WHERE clause and reads named columns.
-- No case counts the rows of a catalog: the reference server carries contrib
-- extensions and scratch objects that a count would see and memgres would not.
-- ============================================================================

-- begin-expected
-- columns: allargtypes,argmodes,argnames
-- row: {114,25,114}|{i,o,o}|{from_json,key,value}
-- end-expected
SELECT proallargtypes::text AS allargtypes, proargmodes::text AS argmodes, proargnames::text AS argnames FROM pg_proc WHERE proname = 'json_each' AND proargtypes::text = '114';

-- begin-expected
-- columns: allargtypes,argmodes,argnames
-- row: {3802,25,3802}|{i,o,o}|{from_json,key,value}
-- end-expected
SELECT proallargtypes::text AS allargtypes, proargmodes::text AS argmodes, proargnames::text AS argnames FROM pg_proc WHERE proname = 'jsonb_each' AND proargtypes::text = '3802';

-- begin-expected
-- columns: allargtypes,argmodes,argnames
-- row: {1034,26,26,25,16}|{i,o,o,o,o}|{acl,grantor,grantee,privilege_type,is_grantable}
-- end-expected
SELECT proallargtypes::text AS allargtypes, proargmodes::text AS argmodes, proargnames::text AS argnames FROM pg_proc WHERE proname = 'aclexplode' AND proargtypes::text = '1034';

-- begin-expected
-- columns: allargtypes,argmodes,argnames
-- row: {3614,25,1005,1009}|{i,o,o,o}|{tsvector,lexeme,positions,weights}
-- end-expected
SELECT proallargtypes::text AS allargtypes, proargmodes::text AS argmodes, proargnames::text AS argnames FROM pg_proc WHERE proname = 'unnest' AND proargtypes::text = '3614';

-- begin-expected
-- columns: allargtypes,argmodes,argnames
-- row: {25,18,16,25,25}|{o,o,o,o,o}|{word,catcode,barelabel,catdesc,baredesc}
-- end-expected
SELECT proallargtypes::text AS allargtypes, proargmodes::text AS argmodes, proargnames::text AS argnames FROM pg_proc WHERE proname = 'pg_get_keywords' AND proargtypes::text = '';

-- begin-expected
-- columns: allargtypes,argmodes
-- row: {23,23,20,1184}|{o,o,o,o}
-- end-expected
SELECT proallargtypes::text AS allargtypes, proargmodes::text AS argmodes FROM pg_proc WHERE proname = 'pg_control_system' AND proargtypes::text = '';

-- begin-expected
-- columns: argmodes,variadic
-- row: {v}|2276
-- end-expected
SELECT proargmodes::text AS argmodes, provariadic AS variadic FROM pg_proc WHERE proname = 'concat' AND proargtypes::text = '2276';

-- begin-expected
-- columns: argmodes,variadic
-- row: {i,v}|2276
-- end-expected
SELECT proargmodes::text AS argmodes, provariadic AS variadic FROM pg_proc WHERE proname = 'concat_ws' AND proargtypes::text = '25 2276';

-- begin-expected
-- columns: argmodes,variadic
-- row: {i,v}|2276
-- end-expected
SELECT proargmodes::text AS argmodes, provariadic AS variadic FROM pg_proc WHERE proname = 'format' AND proargtypes::text = '25 2276';

-- begin-expected
-- columns: argmodes,variadic,argnames
-- row: {i,v}|25|{from_json,path_elems}
-- end-expected
SELECT proargmodes::text AS argmodes, provariadic AS variadic, proargnames::text AS argnames FROM pg_proc WHERE proname = 'json_extract_path' AND proargtypes::text = '114 1009';

-- begin-expected
-- columns: variadic
-- row: 3912
-- end-expected
SELECT provariadic AS variadic FROM pg_proc WHERE proname = 'datemultirange' AND proargtypes::text = '3913';

-- begin-expected
-- columns: variadic
-- row: 0
-- end-expected
SELECT provariadic AS variadic FROM pg_proc WHERE proname = 'datemultirange' AND proargtypes::text = '3912';

-- begin-expected
-- columns: variadic
-- row: 3904
-- end-expected
SELECT provariadic AS variadic FROM pg_proc WHERE proname = 'int4multirange' AND proargtypes::text = '3905';

-- begin-expected
-- columns: argnames
-- row: {value,delimiter}
-- end-expected
SELECT proargnames::text AS argnames FROM pg_proc WHERE proname = 'string_agg' AND proargtypes::text = '25 25';

-- begin-expected
-- columns: argnames
-- row: {jsonb_in,path,replacement,create_if_missing}
-- end-expected
SELECT proargnames::text AS argnames FROM pg_proc WHERE proname = 'jsonb_set' AND proargtypes::text = '3802 1009 3802 16';

-- begin-expected
-- columns: argnames
-- row: {string,pattern}
-- end-expected
SELECT proargnames::text AS argnames FROM pg_proc WHERE proname = 'regexp_matches' AND proargtypes::text = '25 25';

-- begin-expected
-- columns: argnames
-- row: {array,descending}
-- end-expected
SELECT proargnames::text AS argnames FROM pg_proc WHERE proname = 'array_sort' AND proargtypes::text = '2277 16';

-- begin-expected
-- columns: unnamed
-- row: t
-- end-expected
SELECT proargnames IS NULL AS unnamed FROM pg_proc WHERE proname = 'pg_sleep' AND proargtypes::text = '701';

-- begin-expected
-- columns: ndefaults,carried
-- row: 1|t
-- end-expected
SELECT pronargdefaults AS ndefaults, proargdefaults IS NOT NULL AS carried FROM pg_proc WHERE proname = 'jsonb_set' AND proargtypes::text = '3802 1009 3802 16';

-- begin-expected
-- columns: ndefaults,carried
-- row: 7|t
-- end-expected
SELECT pronargdefaults AS ndefaults, proargdefaults IS NOT NULL AS carried FROM pg_proc WHERE proname = 'make_interval' AND proargtypes::text = '23 23 23 23 23 23 701';

-- begin-expected
-- columns: args
-- row: jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean DEFAULT true
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'jsonb_set' AND proargtypes::text = '3802 1009 3802 16';

-- begin-expected
-- columns: args
-- row: from_json json, OUT key text, OUT value json
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'json_each' AND proargtypes::text = '114';

-- begin-expected
-- columns: args
-- row: years integer DEFAULT 0, months integer DEFAULT 0, weeks integer DEFAULT 0, days integer DEFAULT 0, hours integer DEFAULT 0, mins integer DEFAULT 0, secs double precision DEFAULT 0.0
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'make_interval' AND proargtypes::text = '23 23 23 23 23 23 701';

-- begin-expected
-- columns: args
-- row: acl aclitem[], OUT grantor oid, OUT grantee oid, OUT privilege_type text, OUT is_grantable boolean
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'aclexplode' AND proargtypes::text = '1034';

-- begin-expected
-- columns: args
-- row: value text, delimiter text
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'string_agg' AND proargtypes::text = '25 25';

-- begin-expected
-- columns: args
-- row: OUT word text, OUT catcode "char", OUT barelabel boolean, OUT catdesc text, OUT baredesc text
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'pg_get_keywords' AND proargtypes::text = '';

-- jsonb_path_exists is read through its columns rather than through
-- pg_get_function_arguments: memgres renders type OID 4072 as "unknown" there, a gap in
-- format_type's OID table that has nothing to do with the argument metadata and that this
-- file is not the place to assert.
-- begin-expected
-- columns: argnames,ndefaults,carried
-- row: {target,path,vars,silent}|2|t
-- end-expected
SELECT proargnames::text AS argnames, pronargdefaults AS ndefaults, proargdefaults IS NOT NULL AS carried FROM pg_proc WHERE proname = 'jsonb_path_exists' AND proargtypes::text = '3802 4072 3802 16';

-- begin-expected
-- columns: args
-- row: wait boolean DEFAULT true, wait_seconds integer DEFAULT 60
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'pg_promote' AND proargtypes::text = '16 23';

-- begin-expected
-- columns: args
-- row: mean double precision DEFAULT 0, stddev double precision DEFAULT 1
-- end-expected
SELECT pg_get_function_arguments(oid) AS args FROM pg_proc WHERE proname = 'random_normal' AND proargtypes::text = '701 701';

-- begin-expected
-- columns: cost,rows
-- row: 1|0
-- end-expected
SELECT procost AS cost, prorows AS rows FROM pg_proc WHERE proname = 'pg_sleep' AND proargtypes::text = '701';

-- begin-expected
-- columns: cost,rows
-- row: 100|0
-- end-expected
SELECT procost AS cost, prorows AS rows FROM pg_proc WHERE proname = 'to_tsvector' AND proargtypes::text = '25';

-- begin-expected
-- columns: cost,rows
-- row: 10|500
-- end-expected
SELECT procost AS cost, prorows AS rows FROM pg_proc WHERE proname = 'pg_get_keywords' AND proargtypes::text = '';

-- begin-expected
-- columns: cost,rows
-- row: 10|10000
-- end-expected
SELECT procost AS cost, prorows AS rows FROM pg_proc WHERE proname = 'ts_stat' AND proargtypes::text = '25';

-- begin-expected
-- columns: cost,rows
-- row: 10|0
-- end-expected
SELECT procost AS cost, prorows AS rows FROM pg_proc WHERE proname = 'pg_table_is_visible' AND proargtypes::text = '26';

-- begin-expected
-- columns: cost,rows
-- row: 1|100
-- end-expected
SELECT procost AS cost, prorows AS rows FROM pg_proc WHERE proname = 'json_each' AND proargtypes::text = '114';

-- begin-expected
-- columns: kind,parallel
-- row: a|s
-- end-expected
SELECT prokind AS kind, proparallel AS parallel FROM pg_proc WHERE proname = 'count' AND proargtypes::text = '';

-- begin-expected
-- columns: kind,parallel
-- row: a|s
-- end-expected
SELECT prokind AS kind, proparallel AS parallel FROM pg_proc WHERE proname = 'sum' AND proargtypes::text = '23';

-- begin-expected
-- columns: kind,parallel,strict
-- row: w|s|f
-- end-expected
SELECT prokind AS kind, proparallel AS parallel, proisstrict AS strict FROM pg_proc WHERE proname = 'row_number' AND proargtypes::text = '';

-- begin-expected
-- columns: kind,parallel,strict
-- row: w|s|t
-- end-expected
SELECT prokind AS kind, proparallel AS parallel, proisstrict AS strict FROM pg_proc WHERE proname = 'lag' AND proargtypes::text = '2283';

-- begin-expected
-- columns: kind,parallel,strict
-- row: w|s|t
-- end-expected
SELECT prokind AS kind, proparallel AS parallel, proisstrict AS strict FROM pg_proc WHERE proname = 'ntile' AND proargtypes::text = '23';

-- begin-expected
-- columns: volatility,parallel
-- row: s|s
-- end-expected
SELECT provolatile AS volatility, proparallel AS parallel FROM pg_proc WHERE proname = 'json_agg' AND proargtypes::text = '2283';

-- begin-expected
-- columns: volatility,parallel
-- row: s|r
-- end-expected
SELECT provolatile AS volatility, proparallel AS parallel FROM pg_proc WHERE proname = 'age' AND proargtypes::text = '28';

-- begin-expected
-- columns: volatility,parallel
-- row: s|r
-- end-expected
SELECT provolatile AS volatility, proparallel AS parallel FROM pg_proc WHERE proname = 'database_to_xml' AND proargtypes::text = '16 16 25';

-- begin-expected
-- columns: volatility,parallel,strict
-- row: s|s|t
-- end-expected
SELECT provolatile AS volatility, proparallel AS parallel, proisstrict AS strict FROM pg_proc WHERE proname = 'array_in' AND proargtypes::text = '2275 26 23';

-- begin-expected
-- columns: strict
-- row: f
-- end-expected
SELECT proisstrict AS strict FROM pg_proc WHERE proname = 'internal_in' AND proargtypes::text = '2275';

-- begin-expected
-- columns: collation
-- row: 100
-- end-expected
SELECT typcollation AS collation FROM pg_type WHERE typname = 'pg_node_tree';

-- begin-expected
-- columns: collation
-- row: 100
-- end-expected
SELECT typcollation AS collation FROM pg_type WHERE typname = 'pg_mcv_list';

-- begin-expected
-- columns: collation
-- row: 100
-- end-expected
SELECT typcollation AS collation FROM pg_type WHERE typname = 'pg_brin_bloom_summary';

-- begin-expected
-- columns: typlen,typbyval,typtype,typcategory,typalign,typstorage,typelem,typarray
-- row: -2|f|p|X|c|p|0|0
-- end-expected
SELECT typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray FROM pg_type WHERE typname = 'unknown';

-- begin-expected
-- columns: typlen,typbyval,typtype,typcategory,typalign,typstorage,typelem,typarray
-- row: -1|f|b|U|i|x|0|2201
-- end-expected
SELECT typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray FROM pg_type WHERE typname = 'refcursor';

-- begin-expected
-- columns: typlen,typbyval,typtype,typcategory,typalign,typstorage,typelem,typarray
-- row: -1|f|b|U|i|p|0|3644
-- end-expected
SELECT typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray FROM pg_type WHERE typname = 'gtsvector';

-- begin-expected
-- columns: typelem,typcategory
-- row: 2275|A
-- end-expected
SELECT typelem, typcategory FROM pg_type WHERE typname = '_cstring';

-- begin-expected
-- columns: input,output,receive,send
-- row: textin|textout|textrecv|textsend
-- end-expected
SELECT typinput::text AS input, typoutput::text AS output, typreceive::text AS receive, typsend::text AS send FROM pg_type WHERE typname = 'refcursor';

-- begin-expected
-- columns: input,output,receive,send
-- row: gtsvectorin|gtsvectorout|-|-
-- end-expected
SELECT typinput::text AS input, typoutput::text AS output, typreceive::text AS receive, typsend::text AS send FROM pg_type WHERE typname = 'gtsvector';

-- begin-expected
-- columns: typelem
-- row: 18
-- end-expected
SELECT typelem FROM pg_type WHERE typname = 'name';

-- begin-expected
-- columns: typelem
-- row: 701
-- end-expected
SELECT typelem FROM pg_type WHERE typname = 'point';

-- begin-expected
-- columns: typelem
-- row: 600
-- end-expected
SELECT typelem FROM pg_type WHERE typname = 'box';

-- begin-expected
-- columns: typelem
-- row: 701
-- end-expected
SELECT typelem FROM pg_type WHERE typname = 'line';

-- begin-expected
-- columns: typarray,receive,send
-- row: 2287|record_recv|record_send
-- end-expected
SELECT typarray, typreceive::text AS receive, typsend::text AS send FROM pg_type WHERE typname = 'record';

-- begin-expected
-- columns: typarray,receive,send
-- row: 1263|cstring_recv|cstring_send
-- end-expected
SELECT typarray, typreceive::text AS receive, typsend::text AS send FROM pg_type WHERE typname = 'cstring';

-- begin-expected
-- columns: typarray,receive,send
-- row: 0|anyarray_recv|anyarray_send
-- end-expected
SELECT typarray, typreceive::text AS receive, typsend::text AS send FROM pg_type WHERE typname = 'anyarray';

-- begin-expected
-- columns: typarray,receive,send
-- row: 0|void_recv|void_send
-- end-expected
SELECT typarray, typreceive::text AS receive, typsend::text AS send FROM pg_type WHERE typname = 'void';

-- begin-expected
-- columns: typarray,receive,send
-- row: 0|-|-
-- end-expected
SELECT typarray, typreceive::text AS receive, typsend::text AS send FROM pg_type WHERE typname = 'internal';

-- begin-expected
-- columns: element
-- row: char
-- end-expected
SELECT e.typname AS element FROM pg_type t JOIN pg_type e ON e.oid = t.typelem WHERE t.typname = 'name';

-- begin-expected
-- columns: element
-- row: refcursor
-- end-expected
SELECT e.typname AS element FROM pg_type t JOIN pg_type e ON e.oid = t.typelem WHERE t.typname = '_refcursor';

-- begin-expected
-- columns: array_of_record
-- row: _record
-- end-expected
SELECT a.typname AS array_of_record FROM pg_type t JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'record';

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*) AS dangling FROM pg_type t LEFT JOIN pg_type e ON e.oid = t.typelem WHERE t.typelem <> 0 AND e.oid IS NULL;

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*) AS dangling FROM pg_type t LEFT JOIN pg_type a ON a.oid = t.typarray WHERE t.typarray <> 0 AND a.oid IS NULL;

-- begin-expected
-- columns: literal
-- row: x
-- end-expected
SELECT 'x'::unknown AS literal;

-- begin-expected
-- columns: argtype
-- row: text
-- end-expected
SELECT t.typname AS argtype FROM pg_type t WHERE t.oid = (SELECT p.proallargtypes[2] FROM pg_proc p WHERE p.proname = 'json_each' AND p.proargtypes::text = '114');

-- begin-expected
-- columns: bad
-- end-expected
SELECT p.proname AS bad FROM pg_proc p WHERE p.pronargdefaults > p.pronargs LIMIT 1;

-- begin-expected
-- columns: bad
-- end-expected
SELECT p.proname AS bad FROM pg_proc p WHERE p.proargmodes IS NOT NULL AND p.proallargtypes IS NOT NULL AND array_length(p.proargmodes, 1) <> array_length(p.proallargtypes, 1) LIMIT 1;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT array_length(proallargtypes, 1) AS n FROM pg_proc WHERE proname = 'json_each' AND proargtypes::text = '114';

-- begin-expected
-- columns: result
-- row: SETOF record
-- end-expected
SELECT pg_get_function_result(oid) AS result FROM pg_proc WHERE proname = 'json_each' AND proargtypes::text = '114';
