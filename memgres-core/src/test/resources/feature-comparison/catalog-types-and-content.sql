-- ============================================================================
-- Feature Comparison: catalog column types and catalog content
-- Target: PostgreSQL 18 vs Memgres
--
-- What a tool that introspects the server reads, and whether it can act on it:
--   * the object-identifier columns are oid / regproc / oidvector, so a query
--     that compares one to 0, joins on it or takes its length runs at all;
--   * pg_cast lists the conversions PostgreSQL registers and no others, since a
--     phantom implicit cast changes what the server silently accepts;
--   * every information_schema view the catalog lists answers for its columns;
--   * pg_proc rows carry argument and return types, not just a name;
--   * array types are named for what they hold and are linked back from it.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS ctc_t CASCADE;
DROP FUNCTION IF EXISTS ctc_add(int, int) CASCADE;

CREATE TABLE ctc_t (id int PRIMARY KEY, name text);
CREATE FUNCTION ctc_add(a int, b int) RETURNS bigint LANGUAGE sql AS $$ SELECT (a + b)::bigint $$;

-- ============================================================================
-- Catalog column types: PG's own catalog-consistency checks must be runnable
-- ============================================================================

-- typinput/typoutput are regproc, so comparing them to 0 is a valid query.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_type WHERE typinput = 0 OR typoutput = 0;

-- proargtypes is an oidvector, so ANY over it resolves.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_proc WHERE 0 = ANY (proargtypes);

-- and its length is an array length, not a string length.
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_proc
 WHERE proname = 'ctc_add' AND array_length(proargtypes, 1) = 2;

-- The declared type of each object-identifier column.
-- begin-expected
-- columns: pg_typeof
-- row: regproc
-- end-expected
SELECT pg_typeof(typinput)::text AS pg_typeof FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: pg_typeof
-- row: regproc
-- end-expected
SELECT pg_typeof(typoutput)::text AS pg_typeof FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(oid)::text AS pg_typeof FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(typnamespace)::text AS pg_typeof FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(typelem)::text AS pg_typeof FROM pg_type WHERE typname = '_int4';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(typarray)::text AS pg_typeof FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: pg_typeof
-- row: oidvector
-- end-expected
SELECT pg_typeof(proargtypes)::text AS pg_typeof FROM pg_proc WHERE proname = 'ctc_add';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(prorettype)::text AS pg_typeof FROM pg_proc WHERE proname = 'ctc_add';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(oid)::text AS pg_typeof FROM pg_class WHERE relname = 'ctc_t';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(relnamespace)::text AS pg_typeof FROM pg_class WHERE relname = 'ctc_t';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(atttypid)::text AS pg_typeof FROM pg_attribute
 WHERE attrelid = 'ctc_t'::regclass AND attname = 'id';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(oid)::text AS pg_typeof FROM pg_namespace WHERE nspname = 'public';

-- begin-expected
-- columns: pg_typeof
-- row: oid
-- end-expected
SELECT pg_typeof(castsource)::text AS pg_typeof FROM pg_cast
 WHERE castsource = 23 AND casttarget = 20;

-- A regproc column still prints as the function's name.
-- begin-expected
-- columns: typinput | typoutput
-- row: int4in, int4out
-- end-expected
SELECT typinput::text AS typinput, typoutput::text AS typoutput
  FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: typinput
-- row: numeric_in
-- end-expected
SELECT typinput::text AS typinput FROM pg_type WHERE typname = 'numeric';

-- begin-expected
-- columns: typinput
-- row: cash_in
-- end-expected
SELECT typinput::text AS typinput FROM pg_type WHERE typname = 'money';

-- begin-expected
-- columns: typinput
-- row: range_in
-- end-expected
SELECT typinput::text AS typinput FROM pg_type WHERE typname = 'int4range';

-- begin-expected
-- columns: typinput
-- row: multirange_in
-- end-expected
SELECT typinput::text AS typinput FROM pg_type WHERE typname = 'int4multirange';

-- begin-expected
-- columns: typmodin
-- row: numerictypmodin
-- end-expected
SELECT typmodin::text AS typmodin FROM pg_type WHERE typname = 'numeric';

-- A type with no modifier reports PG's InvalidOid spelling, not NULL.
-- begin-expected
-- columns: typmodin
-- row: -
-- end-expected
SELECT typmodin::text AS typmodin FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: typanalyze
-- row: range_typanalyze
-- end-expected
SELECT typanalyze::text AS typanalyze FROM pg_type WHERE typname = 'daterange';

-- begin-expected
-- columns: typanalyze
-- row: array_typanalyze
-- end-expected
SELECT typanalyze::text AS typanalyze FROM pg_type WHERE typname = '_text';

-- begin-expected
-- columns: typsubscript
-- row: array_subscript_handler
-- end-expected
SELECT typsubscript::text AS typsubscript FROM pg_type WHERE typname = '_int4';

-- ============================================================================
-- pg_cast: the phantom conversions
-- ============================================================================

-- PG has no int4->text cast; its absence is what makes '5'::text = 5 an error.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 23 AND casttarget = 25;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource IN (20, 21, 700, 701, 1700) AND casttarget = 25;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 16 AND casttarget = 21;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast
 WHERE casttarget = 3802 AND castsource IN (16, 20, 21, 23, 25, 700, 701, 1700);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 790 AND casttarget IN (23, 20);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 19 AND casttarget IN (23, 20);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 23 AND casttarget = 28;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 602 AND casttarget = 600;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast
 WHERE castsource IN (603, 718, 1082, 1186, 114, 3802, 17, 3614, 600) AND casttarget = 25;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 25
   AND casttarget IN (23, 17, 114, 1114, 3802, 3614, 600, 829);

-- The only same-type entries are the length/precision coercions PG registers.
-- begin-expected
-- columns: count
-- row: 10
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = casttarget;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = casttarget
   AND castsource IN (16, 20, 21, 23, 25, 700, 701);

-- The conversions PG does register keep their context and method.
-- begin-expected
-- columns: castcontext | castmethod
-- row: i, f
-- end-expected
SELECT castcontext::text AS castcontext, castmethod::text AS castmethod
  FROM pg_cast WHERE castsource = 21 AND casttarget = 23;

-- begin-expected
-- columns: castcontext | castmethod
-- row: a, f
-- end-expected
SELECT castcontext::text AS castcontext, castmethod::text AS castmethod
  FROM pg_cast WHERE castsource = 1700 AND casttarget = 23;

-- bpchar->text is implicit in PG, not assignment.
-- begin-expected
-- columns: castcontext
-- row: i
-- end-expected
SELECT castcontext::text AS castcontext FROM pg_cast
 WHERE castsource = 1042 AND casttarget = 25;

-- bit->int4 is explicit in PG, not assignment.
-- begin-expected
-- columns: castcontext
-- row: e
-- end-expected
SELECT castcontext::text AS castcontext FROM pg_cast
 WHERE castsource = 1560 AND casttarget = 23;

-- begin-expected
-- columns: castcontext
-- row: a
-- end-expected
SELECT castcontext::text AS castcontext FROM pg_cast
 WHERE castsource = 16 AND casttarget = 25;

-- begin-expected
-- columns: castmethod
-- row: i
-- end-expected
SELECT castmethod::text AS castmethod FROM pg_cast
 WHERE castsource = 114 AND casttarget = 3802;

-- A cast performed by a function names the function; a binary-coercible one has none.
-- begin-expected
-- columns: castfunc
-- row: 1740
-- end-expected
SELECT castfunc::text AS castfunc FROM pg_cast WHERE castsource = 23 AND casttarget = 1700;

-- begin-expected
-- columns: castfunc
-- row: 0
-- end-expected
SELECT castfunc::text AS castfunc FROM pg_cast WHERE castsource = 23 AND casttarget = 26;

-- ============================================================================
-- information_schema: a listed view answers for its columns
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM information_schema.column_privileges WHERE 1 = 0;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(grantee) AS count FROM information_schema.column_privileges WHERE 1 = 0;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM information_schema.table_privileges WHERE 1 = 0;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT grantor, grantee, table_catalog, table_schema, table_name,
       privilege_type, is_grantable, with_hierarchy
       FROM information_schema.table_privileges WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT object_catalog, object_schema, object_name, object_type,
       dtd_identifier FROM information_schema.data_type_privileges WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT udt_name, attribute_name, ordinal_position, data_type
       FROM information_schema.attributes WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT object_name, data_type, udt_name, dtd_identifier
       FROM information_schema.element_types WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT domain_name, constraint_name, is_deferrable
       FROM information_schema.domain_constraints WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT feature_id, feature_name, is_supported
       FROM information_schema.sql_features WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT sizing_id, sizing_name, supported_value
       FROM information_schema.sql_sizing WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT view_name, table_name, column_name
       FROM information_schema.view_column_usage WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT specific_name, routine_name, table_name
       FROM information_schema.routine_table_usage WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT foreign_server_name, foreign_data_wrapper_name
       FROM information_schema.foreign_servers WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT authorization_identifier, foreign_server_name
       FROM information_schema.user_mappings WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT collation_name, character_set_name
       FROM information_schema.collation_character_set_applicability WHERE 1 = 0) s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT user_defined_type_name, user_defined_type_category
       FROM information_schema.user_defined_types WHERE 1 = 0) s;

-- The one-row views carry their row.
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM information_schema.information_schema_catalog_name;

-- begin-expected
-- columns: character_set_name | character_repertoire | form_of_use
-- row: UTF8, UCS, UTF8
-- end-expected
SELECT character_set_name, character_repertoire, form_of_use
  FROM information_schema.character_sets;

-- And the columns of a listed view are themselves listed.
-- begin-expected
-- columns: column_name
-- row: grantor
-- row: grantee
-- row: table_catalog
-- row: table_schema
-- row: table_name
-- row: column_name
-- row: privilege_type
-- row: is_grantable
-- end-expected
SELECT column_name FROM information_schema.columns
 WHERE table_schema = 'information_schema' AND table_name = 'column_privileges'
 ORDER BY ordinal_position;

-- begin-expected
-- columns: count
-- row: 7
-- end-expected
SELECT count(*) FROM information_schema.columns
 WHERE table_schema = 'information_schema' AND table_name = 'sql_features';

-- ============================================================================
-- pg_proc: rows carry a signature
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'upper' AND prorettype = 25;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'now' AND prorettype = 1184;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'lower' AND prorettype = 25;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'abs' AND prorettype = 23 AND pronargs = 1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'sqrt' AND prorettype = 701;

-- Every overload PG declares for a name memgres implements is registered.
-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'abs';

-- begin-expected
-- columns: count
-- row: 8
-- end-expected
SELECT count(*) FROM pg_proc WHERE proname = 'length';

-- begin-expected
-- columns: proargtypes
-- row: 25
-- row: 3831
-- row: 4537
-- end-expected
SELECT proargtypes::text AS proargtypes FROM pg_proc
 WHERE proname = 'upper' ORDER BY proargtypes::text;

-- begin-expected
-- columns: proargtypes
-- row: 1114 25
-- row: 1184 25
-- row: 1186 25
-- row: 1700 25
-- row: 20 25
-- row: 23 25
-- row: 700 25
-- row: 701 25
-- end-expected
SELECT proargtypes::text AS proargtypes FROM pg_proc
 WHERE proname = 'to_char' ORDER BY proargtypes::text;

-- A user-defined function carries its own signature.
-- begin-expected
-- columns: pronargs | prorettype | proargtypes
-- row: 2, 20, 23 23
-- end-expected
SELECT pronargs, prorettype::text AS prorettype, proargtypes::text AS proargtypes
  FROM pg_proc WHERE proname = 'ctc_add';

-- begin-expected
-- columns: unnest
-- row: 23
-- row: 23
-- end-expected
SELECT unnest(proargtypes)::text AS unnest FROM pg_proc WHERE proname = 'ctc_add';

-- ============================================================================
-- Array types: named for what they hold, and linked back from it
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_type WHERE typname ~ '^_[0-9]+$';

-- begin-expected
-- columns: typname
-- row: _char
-- end-expected
SELECT typname FROM pg_type WHERE oid = 1002;

-- begin-expected
-- columns: typname
-- row: _regclass
-- end-expected
SELECT typname FROM pg_type WHERE oid = 2210;

-- begin-expected
-- columns: typname
-- row: _regtype
-- end-expected
SELECT typname FROM pg_type WHERE oid = 2211;

-- begin-expected
-- columns: typname
-- row: _regproc
-- end-expected
SELECT typname FROM pg_type WHERE oid = 1008;

-- begin-expected
-- columns: typname
-- row: _oid
-- end-expected
SELECT typname FROM pg_type WHERE oid = 1028;

-- Following typarray from an element type finds its array type, which is how a
-- driver discovers array support for a type it does not hardcode.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_type t1
 WHERE t1.typname IN ('int2','int4','int8','float4','float8','numeric','text','varchar',
                      'bpchar','name','bool','date','time','timetz','timestamp','timestamptz',
                      'interval','uuid','bytea','json','jsonb','inet','cidr','macaddr','macaddr8',
                      'oid','money','bit','varbit','xml','point','lseg','path','box','polygon',
                      'circle','line','tsvector','tsquery','int4range','int8range','numrange',
                      'daterange','tsrange','tstzrange','regproc','regclass','regtype',
                      'oidvector','int2vector','xid')
   AND NOT EXISTS (SELECT 1 FROM pg_type t2
                    WHERE t2.typname = '_' || t1.typname
                      AND t2.typelem = t1.oid AND t1.typarray = t2.oid);

-- begin-expected
-- columns: typarray
-- row: 1005
-- end-expected
SELECT typarray::text AS typarray FROM pg_type WHERE typname = 'int2';

-- begin-expected
-- columns: typarray
-- row: 1231
-- end-expected
SELECT typarray::text AS typarray FROM pg_type WHERE typname = 'numeric';

-- begin-expected
-- columns: typarray
-- row: 1028
-- end-expected
SELECT typarray::text AS typarray FROM pg_type WHERE typname = 'oid';

-- begin-expected
-- columns: typname
-- row: _int2
-- end-expected
SELECT typname FROM pg_type WHERE oid = (SELECT typarray FROM pg_type WHERE typname = 'int2');

-- begin-expected
-- columns: typname
-- row: _numeric
-- end-expected
SELECT typname FROM pg_type WHERE oid = (SELECT typarray FROM pg_type WHERE typname = 'numeric');

-- The object-identifier types themselves are registered.
-- begin-expected
-- columns: count
-- row: 5
-- end-expected
SELECT count(*) FROM pg_type
 WHERE typname IN ('regproc','regclass','regtype','oidvector','int2vector');

-- begin-expected
-- columns: typlen | typcategory
-- row: 4, N
-- end-expected
SELECT typlen, typcategory::text AS typcategory FROM pg_type WHERE typname = 'regproc';

-- begin-expected
-- columns: typcategory | typelem
-- row: A, 26
-- end-expected
SELECT typcategory::text AS typcategory, typelem::text AS typelem
  FROM pg_type WHERE typname = 'oidvector';

-- ============================================================================
-- Neighbouring catalog behaviour that must not change
-- ============================================================================

-- begin-expected
-- columns: nspname
-- row: pg_catalog
-- end-expected
SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE p.proname = 'upper';

-- begin-expected
-- columns: typname
-- row: int4
-- end-expected
SELECT e.typname FROM pg_type a JOIN pg_type e ON e.oid = a.typelem WHERE a.typname = '_int4';

-- begin-expected
-- columns: t
-- row: r
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'int4range';

-- begin-expected
-- columns: t
-- row: m
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'int4multirange';

-- begin-expected
-- columns: t
-- row: b
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: t
-- row: c
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'ctc_t';

-- begin-expected
-- columns: relname
-- row: ctc_t
-- end-expected
SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public' AND c.relname = 'ctc_t';

-- begin-expected
-- columns: attname
-- row: id
-- row: name
-- end-expected
SELECT a.attname FROM pg_attribute a
 WHERE a.attrelid = 'ctc_t'::regclass AND a.attnum > 0 ORDER BY a.attnum;

-- begin-expected
-- columns: t
-- row: int4
-- end-expected
SELECT t.typname AS t FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
 WHERE a.attrelid = 'ctc_t'::regclass AND a.attname = 'id';

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_cast WHERE castsource = 23 AND casttarget = 20 AND castcontext = 'i';

DROP FUNCTION ctc_add(int, int);
DROP TABLE ctc_t;
