-- ============================================================================
-- Feature Comparison: the catalog agrees with itself and its references resolve
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Describing pg_catalog was half the job. The listing in information_schema
-- stopped at the first column of a type the type-name mapping did not know, and
-- the caller turned that into missing rows rather than an error, so
-- pg_attribute and information_schema contradicted each other about the same
-- relation. Selecting from a relation returned one column more than
-- pg_attribute declared. Attributes were numbered in memgres's own order rather
-- than PostgreSQL's, and declared with memgres's own types -- oid as integer,
-- name as text, a parse tree as text. And references that had just been made to
-- resolve pointed at rows that were never added.
--
-- The checks are shape and reference invariants, not row counts: a live
-- server's totals move with its version and its installed extensions.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS pcx_c CASCADE;
DROP TABLE IF EXISTS pcx_t CASCADE;
DROP TYPE IF EXISTS pcx_mood CASCADE;
CREATE TYPE pcx_mood AS ENUM ('sad','ok','happy');
CREATE TABLE pcx_t (id integer PRIMARY KEY, code varchar(10) NOT NULL UNIQUE,
                    descr text, amount numeric(10,2) DEFAULT 0.00,
                    created timestamp without time zone,
                    flag boolean DEFAULT false, m pcx_mood, arr integer[]);
CREATE TABLE pcx_c (cid bigserial PRIMARY KEY, pid integer);

-- ============================================================================
-- 1. information_schema lists every column of every catalog relation
-- ============================================================================

-- begin-expected
-- columns: truncated
-- row: 0
-- end-expected
SELECT count(*) AS truncated
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog' AND c.relkind IN ('r','v')
  AND c.relnatts <> (SELECT count(*) FROM information_schema.columns i
                     WHERE i.table_schema = 'pg_catalog'
                       AND i.table_name = c.relname);

-- pg_type is listed in full, in the order PostgreSQL numbers its columns
-- begin-expected
-- columns: cols
-- row: oid,typname,typnamespace,typowner,typlen,typbyval,typtype,typcategory,typispreferred,typisdefined,typdelim,typrelid,typsubscript,typelem,typarray,typinput,typoutput,typreceive,typsend,typmodin,typmodout,typanalyze,typalign,typstorage,typnotnull,typbasetype,typtypmod,typndims,typcollation,typdefaultbin,typdefault,typacl
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols
FROM information_schema.columns
WHERE table_schema = 'pg_catalog' AND table_name = 'pg_type';

-- ... and the catalog relations themselves are listed as relations
-- begin-expected
-- columns: table_name | table_type
-- row: pg_class, BASE TABLE
-- row: pg_tables, VIEW
-- end-expected
SELECT table_name, table_type FROM information_schema.tables
WHERE table_schema = 'pg_catalog' AND table_name IN ('pg_class','pg_tables')
ORDER BY table_name;

-- ============================================================================
-- 2. Attributes are numbered the way PostgreSQL numbers them
-- ============================================================================

-- begin-expected
-- columns: cols
-- row: attrelid,attname,atttypid,attlen,attnum,atttypmod,attndims,attbyval,attalign,attstorage,attcompression,attnotnull,atthasdef,atthasmissing,attidentity,attgenerated,attisdropped,attislocal,attinhcount,attcollation,attstattarget,attacl,attoptions,attfdwoptions,attmissingval
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum) AS cols
FROM pg_attribute WHERE attrelid = 'pg_attribute'::regclass AND attnum > 0;

-- begin-expected
-- columns: cols
-- row: indexrelid,indrelid,indnatts,indnkeyatts,indisunique,indnullsnotdistinct,indisprimary,indisexclusion,indimmediate,indisclustered,indisvalid,indcheckxmin,indisready,indislive,indisreplident,indkey,indcollation,indclass,indoption,indexprs,indpred
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum) AS cols
FROM pg_attribute WHERE attrelid = 'pg_index'::regclass AND attnum > 0;

-- begin-expected
-- columns: cols
-- row: oid,tgrelid,tgparentid,tgname,tgfoid,tgtype,tgenabled,tgisinternal,tgconstrrelid,tgconstrindid,tgconstraint,tgdeferrable,tginitdeferred,tgnargs,tgattr,tgargs,tgqual,tgoldtable,tgnewtable
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum) AS cols
FROM pg_attribute WHERE attrelid = 'pg_trigger'::regclass AND attnum > 0;

-- ============================================================================
-- 3. ... and declared with the types PostgreSQL declares them with
-- ============================================================================

-- begin-expected
-- columns: col
-- row: oid oid
-- row: conname name
-- row: connamespace oid
-- row: contype "char"
-- row: condeferrable boolean
-- row: condeferred boolean
-- row: conenforced boolean
-- row: convalidated boolean
-- row: conrelid oid
-- row: contypid oid
-- row: conindid oid
-- row: conparentid oid
-- row: confrelid oid
-- row: confupdtype "char"
-- row: confdeltype "char"
-- row: confmatchtype "char"
-- row: conislocal boolean
-- row: coninhcount smallint
-- row: connoinherit boolean
-- row: conperiod boolean
-- row: conkey smallint[]
-- row: confkey smallint[]
-- row: conpfeqop oid[]
-- row: conppeqop oid[]
-- row: conffeqop oid[]
-- row: confdelsetcols smallint[]
-- row: conexclop oid[]
-- row: conbin pg_node_tree
-- end-expected
SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod) AS col
FROM pg_attribute a
WHERE a.attrelid = 'pg_constraint'::regclass AND a.attnum > 0
ORDER BY a.attnum;

-- a rule's and a policy's expressions are parse trees, not text
-- begin-expected
-- columns: col
-- row: ev_action pg_node_tree
-- row: ev_qual pg_node_tree
-- row: polqual pg_node_tree
-- row: polwithcheck pg_node_tree
-- end-expected
SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod) AS col
FROM pg_attribute a
WHERE a.attrelid IN ('pg_rewrite'::regclass, 'pg_policy'::regclass)
  AND a.attname IN ('ev_qual','ev_action','polqual','polwithcheck')
ORDER BY a.attname;

-- a name column is a name, and an enum's sort order is a real
-- begin-expected
-- columns: col
-- row: enumlabel name
-- row: enumsortorder real
-- row: nspname name
-- end-expected
SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod) AS col
FROM pg_attribute a
WHERE (a.attrelid = 'pg_namespace'::regclass AND a.attname = 'nspname')
   OR (a.attrelid = 'pg_enum'::regclass
       AND a.attname IN ('enumsortorder','enumlabel'))
ORDER BY a.attname;

-- ============================================================================
-- 4. A reference the catalog makes reaches something
-- ============================================================================

-- begin-expected
-- columns: dangling_typarray
-- row: 0
-- end-expected
SELECT count(*) AS dangling_typarray FROM pg_type t
WHERE t.typarray <> 0
  AND NOT EXISTS (SELECT 1 FROM pg_type x WHERE x.oid = t.typarray);

-- begin-expected
-- columns: dangling_io
-- row: 0
-- end-expected
SELECT count(*) AS dangling_io FROM pg_type t
WHERE t.typinput <> 0
  AND NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = t.typinput);

-- an aggregate pg_proc reports has a pg_aggregate row behind it
-- begin-expected
-- columns: unregistered
-- row: 0
-- end-expected
SELECT count(*) AS unregistered FROM pg_proc
WHERE prokind = 'a'
  AND NOT EXISTS (SELECT 1 FROM pg_aggregate a WHERE a.aggfnoid = pg_proc.oid);

-- and every relation has the row type pg_class.reltype names
-- begin-expected
-- columns: relname | typname
-- row: pg_attribute, pg_attribute
-- row: pg_class, pg_class
-- row: pg_type, pg_type
-- end-expected
SELECT c.relname, t.typname FROM pg_class c JOIN pg_type t ON t.oid = c.reltype
WHERE c.relname IN ('pg_class','pg_attribute','pg_type')
ORDER BY c.relname;

-- ============================================================================
-- 5. An OID read out of a catalog column reads back as a name
-- ============================================================================

-- begin-expected
-- columns: name_type | char_type | node_tree | jsonb_array
-- row: name, "char", pg_node_tree, jsonb[]
-- end-expected
SELECT 19::regtype::text AS name_type, 18::regtype::text AS char_type,
       194::regtype::text AS node_tree, 3807::regtype::text AS jsonb_array;

-- begin-expected
-- columns: unnamed_types
-- row: 0
-- end-expected
SELECT count(*) AS unnamed_types FROM pg_type t
WHERE t.oid::regtype::text ~ '^[0-9]+$';

-- begin-expected
-- columns: unnamed_procs
-- row: 0
-- end-expected
SELECT count(*) AS unnamed_procs FROM pg_proc p
WHERE p.oid::regproc::text ~ '^[0-9]+$';

-- an operator names the function behind it
-- begin-expected
-- columns: oprname | l | r | res | code
-- row: =, text, name, boolean, texteqname
-- end-expected
SELECT oprname, oprleft::regtype::text AS l, oprright::regtype::text AS r,
       oprresult::regtype::text AS res, oprcode::text AS code
FROM pg_operator
WHERE oprname = '=' AND oprleft = 'text'::regtype AND oprright = 'name'::regtype;

-- ============================================================================
-- 6. A column's storage properties are its type's
-- ============================================================================

-- begin-expected
-- columns: attname | attlen | attbyval | attalign | attstorage
-- row: id, 4, t, i, p
-- row: code, -1, f, i, x
-- row: descr, -1, f, i, x
-- row: amount, -1, f, i, m
-- row: created, 8, t, d, p
-- row: flag, 1, t, c, p
-- row: m, 4, t, i, p
-- row: arr, -1, f, i, x
-- end-expected
SELECT a.attname, a.attlen, a.attbyval, a.attalign, a.attstorage
FROM pg_attribute a
WHERE a.attrelid = 'pcx_t'::regclass AND a.attnum > 0
ORDER BY a.attnum;

-- a bigserial is eight bytes, passed by value, double-aligned
-- begin-expected
-- columns: attbyval | attalign | attstorage
-- row: t, d, p
-- end-expected
SELECT attbyval, attalign, attstorage FROM pg_attribute
WHERE attrelid = 'pcx_c'::regclass AND attname = 'cid';

-- ... and pg_type agrees about the widths behind them
-- begin-expected
-- columns: typname | typcategory | typlen | typbyval
-- row: date, D, 4, t
-- row: interval, T, 16, f
-- row: oid, N, 4, t
-- row: time, D, 8, t
-- row: timestamp, D, 8, t
-- row: timestamptz, D, 8, t
-- row: uuid, U, 16, f
-- end-expected
SELECT typname, typcategory, typlen, typbyval FROM pg_type
WHERE typname IN ('date','time','timestamp','timestamptz','uuid','oid','interval')
ORDER BY typname;

-- an array column records one dimension
-- begin-expected
-- columns: attname | attndims
-- row: arr, 1
-- row: descr, 0
-- end-expected
SELECT attname, attndims FROM pg_attribute
WHERE attrelid = 'pcx_t'::regclass AND attname IN ('arr','descr')
ORDER BY attname;

-- ============================================================================
-- 7. What a built-in function tells a planner about itself
-- ============================================================================

-- begin-expected
-- columns: proname | proisstrict | proparallel | provolatile
-- row: now, t, s, s
-- row: random, t, r, v
-- row: upper, t, s, i
-- end-expected
SELECT DISTINCT proname, proisstrict, proparallel, provolatile FROM pg_proc
WHERE proname IN ('now','upper','random') AND prokind = 'f' AND pronargs <= 1
ORDER BY proname;

-- varchar_ops is keyed on text, and is not the default class for either method
-- begin-expected
-- columns: amname | opcdefault | opcintype
-- row: btree, f, text
-- row: hash, f, text
-- end-expected
SELECT am.amname, o.opcdefault, o.opcintype::regtype::text AS opcintype
FROM pg_opclass o JOIN pg_am am ON am.oid = o.opcmethod
WHERE o.opcname = 'varchar_ops' AND am.amname IN ('btree','hash')
ORDER BY am.amname;

-- ============================================================================
-- Teardown
-- ============================================================================

DROP TABLE pcx_c;
DROP TABLE pcx_t;
DROP TYPE pcx_mood;
