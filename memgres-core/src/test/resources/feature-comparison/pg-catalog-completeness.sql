-- ============================================================================
-- Feature Comparison: pg_catalog describes itself and what it holds
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A catalog that cannot be introspected is only half a catalog. Every
-- pg_catalog relation used to have no pg_attribute rows at all, so a driver, an
-- ORM's schema reflection or psql's \d got nothing back for any of them; the
-- rows that did exist pointed at things no other catalog had -- operators with
-- no operand or result type, casts naming types and functions that were absent,
-- and functions with no return type. The checks below are reference and shape
-- invariants rather than row counts, because a live server's totals move with
-- its version and its installed extensions.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS pcc_t CASCADE;
CREATE TABLE pcc_t (id int PRIMARY KEY, name text NOT NULL, amt numeric(10,2));
CREATE INDEX pcc_x ON pcc_t (lower(name));

-- ============================================================================
-- 1. Every catalog table has its attributes
-- ============================================================================

-- begin-expected
-- columns: undescribed
-- row: 0
-- end-expected
SELECT count(*) AS undescribed
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog' AND c.relkind = 'r'
  AND NOT EXISTS (SELECT 1 FROM pg_attribute a
                  WHERE a.attrelid = c.oid AND a.attnum > 0);

-- relnatts has to agree with the number of attribute rows behind it
-- begin-expected
-- columns: disagreeing
-- row: 0
-- end-expected
SELECT count(*) AS disagreeing
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog' AND c.relkind IN ('r','v')
  AND c.relnatts <> (SELECT count(*) FROM pg_attribute a
                     WHERE a.attrelid = c.oid AND a.attnum > 0);

-- and an attribute has to name a type that exists
-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*) AS dangling
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog' AND a.attnum > 0
  AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = a.atttypid);

-- ============================================================================
-- 2. pg_class describes itself
-- ============================================================================

-- begin-expected
-- columns: col
-- row: oid oid
-- row: relacl aclitem[]
-- row: relallfrozen integer
-- row: relallvisible integer
-- row: relam oid
-- row: relchecks smallint
-- row: relfilenode oid
-- row: relforcerowsecurity boolean
-- row: relfrozenxid xid
-- row: relhasindex boolean
-- row: relhasrules boolean
-- row: relhassubclass boolean
-- row: relhastriggers boolean
-- row: relispartition boolean
-- row: relispopulated boolean
-- row: relisshared boolean
-- row: relkind "char"
-- row: relminmxid xid
-- row: relname name
-- row: relnamespace oid
-- row: relnatts smallint
-- row: reloftype oid
-- row: reloptions text[]
-- row: relowner oid
-- row: relpages integer
-- row: relpartbound pg_node_tree
-- row: relpersistence "char"
-- row: relreplident "char"
-- row: relrewrite oid
-- row: relrowsecurity boolean
-- row: reltablespace oid
-- row: reltoastrelid oid
-- row: reltuples real
-- row: reltype oid
-- end-expected
SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod) AS col
FROM pg_attribute a
WHERE a.attrelid = 'pg_class'::regclass AND a.attnum > 0
ORDER BY a.attname;

-- pg_cast is six columns wide and two of them are the single-byte "char"
-- begin-expected
-- columns: col
-- row: castcontext "char"
-- row: castfunc oid
-- row: castmethod "char"
-- row: castsource oid
-- row: casttarget oid
-- row: oid oid
-- end-expected
SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod) AS col
FROM pg_attribute a
WHERE a.attrelid = 'pg_cast'::regclass AND a.attnum > 0
ORDER BY a.attname;

-- ============================================================================
-- 3. A catalog view is not reported as a table
-- ============================================================================

-- begin-expected
-- columns: relname | relkind
-- row: pg_class, r
-- row: pg_proc, r
-- row: pg_settings, v
-- row: pg_stats, v
-- row: pg_tables, v
-- end-expected
SELECT relname, relkind FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog'
  AND relname IN ('pg_class','pg_proc','pg_tables','pg_stats','pg_settings')
ORDER BY relname;

-- ============================================================================
-- 4. The columns tools read are there
-- ============================================================================

-- Row-level-security auditing reads these two, and a query joining on them
-- failed outright rather than reporting no RLS.
-- begin-expected
-- columns: rowsecurity
-- row: f
-- end-expected
SELECT rowsecurity FROM pg_tables WHERE tablename = 'pcc_t';

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(usebypassrls) >= 0 AS ok FROM pg_user;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(attbyval) >= 0 AS ok FROM pg_attribute;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(indcheckxmin) >= 0 AS ok FROM pg_index;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(aggfinalmodify) + count(aggmfinalmodify) >= 0 AS ok FROM pg_aggregate;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(stakind1) + count(staop5) + count(stanumbers3) >= 0 AS ok FROM pg_statistic;

-- ============================================================================
-- 5. pg_operator identifies the operators it lists
-- ============================================================================

-- begin-expected
-- columns: no_result
-- row: 0
-- end-expected
-- scoped to pg_catalog's own operators: a CREATE OPERATOR from another file in this run is
-- not what this section is about.
SELECT count(*) AS no_result FROM pg_operator o
  JOIN pg_namespace n ON n.oid = o.oprnamespace
WHERE n.nspname = 'pg_catalog' AND (o.oprname = '' OR o.oprresult = 0);

-- begin-expected
-- columns: no_operands
-- row: 0
-- end-expected
SELECT count(*) AS no_operands FROM pg_operator
WHERE oprkind = 'b' AND (oprleft = 0 OR oprright = 0);

-- IS is syntax rather than an operator, and PostgreSQL spells the inequality
-- operator <>; != is a parser alias with no catalog row of its own.
-- begin-expected
-- columns: phantoms
-- row: 0
-- end-expected
SELECT count(*) AS phantoms FROM pg_operator WHERE oprname IN ('IS', '!=');

-- an operator's function and result type both have to resolve
-- begin-expected
-- columns: unresolved
-- row: 0
-- end-expected
SELECT count(*) AS unresolved FROM pg_operator o
JOIN pg_namespace n ON n.oid = o.oprnamespace
LEFT JOIN pg_proc p ON p.oid = o.oprcode
WHERE n.nspname = 'pg_catalog'
  AND ((o.oprcode <> 0 AND p.oid IS NULL)
   OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprresult));

-- the arithmetic and comparison operators over int4 carry their signature
-- begin-expected
-- columns: oprname | left | right | result
-- row: *, int4, int4, int4
-- row: +, int4, int4, int4
-- row: -, int4, int4, int4
-- row: /, int4, int4, int4
-- row: <, int4, int4, bool
-- row: <>, int4, int4, bool
-- row: =, int4, int4, bool
-- row: >, int4, int4, bool
-- end-expected
SELECT o.oprname, lt.typname AS "left", rt.typname AS "right", rs.typname AS result
FROM pg_operator o
JOIN pg_type lt ON lt.oid = o.oprleft
JOIN pg_type rt ON rt.oid = o.oprright
JOIN pg_type rs ON rs.oid = o.oprresult
WHERE o.oprname IN ('+','-','*','/','<','=','>','<>')
  AND lt.typname = 'int4' AND rt.typname = 'int4'
ORDER BY o.oprname;

-- ============================================================================
-- 6. pg_opclass, pg_opfamily, pg_amop and pg_collation
-- ============================================================================

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*) AS dangling FROM pg_opclass c
WHERE NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = c.opcmethod)
   OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.opcintype)
   OR NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = c.opcfamily)
   OR NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.oid = c.opcnamespace);

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*) AS dangling FROM pg_amop o
WHERE NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = o.amopfamily)
   OR NOT EXISTS (SELECT 1 FROM pg_operator x WHERE x.oid = o.amopopr);

-- varchar_ops exists but is not the default btree class for varchar
-- begin-expected
-- columns: opcname | opcdefault
-- row: text_ops, t
-- row: varchar_ops, f
-- end-expected
SELECT c.opcname, c.opcdefault FROM pg_opclass c
JOIN pg_am a ON a.oid = c.opcmethod
WHERE a.amname = 'btree' AND c.opcname IN ('text_ops', 'varchar_ops')
ORDER BY c.opcname;

-- gist has no inet_ops family
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_opfamily f JOIN pg_am a ON a.oid = f.opfmethod
WHERE f.opfname = 'inet_ops' AND a.amname = 'gist';

-- A collation the engine will not honour is a hazard, not clutter: a tool that reads
-- pg_collation and offers it writes SQL that fails. Which locale-derived names a server has is
-- a property of the machine it runs on -- a Linux build carries C.UTF-8 and en_US.utf8, a
-- Windows one carries neither -- so what is asserted here is the set every installation has,
-- and that each of them is a name COLLATE actually accepts.
-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*) AS n FROM pg_collation
WHERE collname IN ('C', 'POSIX', 'ucs_basic', 'default');

-- begin-expected
-- columns: c
-- row: a
-- end-expected
SELECT 'a' COLLATE "C" AS c;

-- begin-expected
-- columns: c
-- row: a
-- end-expected
SELECT 'a' COLLATE "ucs_basic" AS c;

-- begin-expected
-- columns: collname | collprovider | collencoding
-- row: C, c, -1
-- row: POSIX, c, -1
-- row: default, d, -1
-- row: ucs_basic, b, 6
-- end-expected
SELECT collname, collprovider, collencoding FROM pg_collation
WHERE collname IN ('default','C','POSIX','ucs_basic')
ORDER BY collname;

-- an ICU locale is spelled with a hyphen, which is what a client passes to COLLATE
-- begin-expected
-- columns: colllocale
-- row: en-US
-- end-expected
SELECT colllocale FROM pg_collation WHERE collname = 'en-US-x-icu';

-- ============================================================================
-- 7. pg_cast names types and functions that exist
-- ============================================================================

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*) AS dangling FROM pg_cast c
WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.castsource)
   OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.casttarget);

-- begin-expected
-- columns: unresolved
-- row: 0
-- end-expected
SELECT count(*) AS unresolved FROM pg_cast c
LEFT JOIN pg_proc p ON p.oid = c.castfunc
WHERE c.castfunc <> 0 AND p.oid IS NULL;

-- The context decides when a cast applies at all, so it is not decoration: an
-- implicit cast reported as assignment will not be applied where PG applies it.
-- begin-expected
-- columns: conversion
-- row: bit->int4 ef
-- row: bool->text af
-- row: bpchar->text if
-- row: date->timestamp if
-- row: int4->float8 if
-- row: int4->int8 if
-- row: int4->numeric if
-- row: int8->int4 af
-- row: text->varchar ib
-- row: varchar->text ib
-- end-expected
SELECT s.typname || '->' || t.typname || ' '
       || c.castcontext::text || c.castmethod::text AS conversion
FROM pg_cast c
JOIN pg_type s ON s.oid = c.castsource
JOIN pg_type t ON t.oid = c.casttarget
WHERE (s.typname, t.typname) IN
      (('bpchar','text'),('bool','text'),('bit','int4'),('int4','int8'),
       ('int4','numeric'),('text','varchar'),('varchar','text'),
       ('int4','float8'),('date','timestamp'),('int8','int4'))
ORDER BY 1;

-- int4 -> text is deliberately absent, which is what makes '5'::text = 5 an error
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_cast c
JOIN pg_type s ON s.oid = c.castsource
JOIN pg_type t ON t.oid = c.casttarget
WHERE s.typname = 'int4' AND t.typname = 'text';

-- ============================================================================
-- 8. pg_proc carries a signature, not just a name
-- ============================================================================

-- begin-expected
-- columns: no_return_type
-- row: 0
-- end-expected
SELECT count(*) AS no_return_type FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'pg_catalog' AND p.prorettype = 0;

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*) AS dangling FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'pg_catalog'
  AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = p.prorettype);

-- a function's argument count agrees with its own argument type list
-- begin-expected
-- columns: inconsistent
-- row: 0
-- end-expected
SELECT count(*) AS inconsistent FROM pg_proc WHERE prokind = 'f'
  AND pronargs <> coalesce(array_length(string_to_array(trim(proargtypes::text), ' '), 1), 0);

-- An aggregate registered as returning anyelement tells a caller nothing:
-- sum(int8) returns numeric while sum(int4) returns bigint.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE prokind = 'a' AND prorettype = 2276;

-- begin-expected
-- columns: signature
-- row: avg(20) -> numeric
-- row: avg(21) -> numeric
-- row: avg(23) -> numeric
-- row: avg(700) -> float8
-- row: avg(701) -> float8
-- row: avg(1700) -> numeric
-- row: max(20) -> int8
-- row: max(21) -> int2
-- row: max(23) -> int4
-- row: max(25) -> text
-- row: max(700) -> float4
-- row: max(701) -> float8
-- row: max(1082) -> date
-- row: max(1700) -> numeric
-- row: min(20) -> int8
-- row: min(21) -> int2
-- row: min(23) -> int4
-- row: min(25) -> text
-- row: min(700) -> float4
-- row: min(701) -> float8
-- row: min(1082) -> date
-- row: min(1700) -> numeric
-- row: sum(20) -> numeric
-- row: sum(21) -> int8
-- row: sum(23) -> int8
-- row: sum(700) -> float4
-- row: sum(701) -> float8
-- row: sum(1700) -> numeric
-- end-expected
SELECT p.proname || '(' || p.proargtypes::text || ') -> ' || t.typname AS signature
FROM pg_proc p JOIN pg_type t ON t.oid = p.prorettype
WHERE p.proname IN ('min','max','sum','avg') AND p.prokind = 'a'
  AND p.proargtypes::text IN ('20','21','23','25','700','701','1082','1700')
ORDER BY p.proname, p.proargtypes::text::int;

-- A window function cannot be called as an ordinary one, and a catalog that
-- reports it as prokind 'f' invites exactly that call.
-- begin-expected
-- columns: fn
-- row: cume_dist float8
-- row: dense_rank int8
-- row: percent_rank float8
-- row: rank int8
-- row: row_number int8
-- end-expected
SELECT p.proname || ' ' || t.typname AS fn FROM pg_proc p
JOIN pg_type t ON t.oid = p.prorettype
WHERE p.prokind = 'w' AND p.pronargs = 0
ORDER BY p.proname;

-- ============================================================================
-- 9. pg_index types an expression column as a parse tree
-- ============================================================================

-- pg_node_tree is what tells a client the value is an internal parse tree and
-- not text meant to be read.
-- begin-expected
-- columns: col
-- row: indcheckxmin boolean
-- row: indexprs pg_node_tree
-- row: indpred pg_node_tree
-- end-expected
SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod) AS col
FROM pg_attribute a
WHERE a.attrelid = 'pg_index'::regclass
  AND a.attname IN ('indexprs','indpred','indcheckxmin')
ORDER BY a.attname;

-- begin-expected
-- columns: typname | typlen | typtype | typcategory
-- row: pg_node_tree, -1, b, Z
-- end-expected
SELECT typname, typlen, typtype, typcategory FROM pg_type
WHERE typname = 'pg_node_tree';

-- begin-expected
-- columns: relname | has_expr
-- row: pcc_x, t
-- end-expected
SELECT c.relname, (i.indexprs IS NOT NULL) AS has_expr
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'pcc_x';

-- ============================================================================
-- 10. Cross-catalog references resolve
-- ============================================================================

-- begin-expected
-- columns: orphan_attrs
-- row: 0
-- end-expected
SELECT count(*) AS orphan_attrs FROM pg_attribute a
WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = a.attrelid);

-- begin-expected
-- columns: orphan_typrelid
-- row: 0
-- end-expected
SELECT count(*) AS orphan_typrelid FROM pg_type t
WHERE t.typrelid <> 0
  AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = t.typrelid);

-- begin-expected
-- columns: rngtype | subtype
-- row: daterange, date
-- row: int4range, int4
-- row: int8range, int8
-- row: numrange, numeric
-- row: tsrange, timestamp
-- row: tstzrange, timestamptz
-- end-expected
SELECT t.typname AS rngtype, st.typname AS subtype FROM pg_range r
JOIN pg_type t ON t.oid = r.rngtypid
JOIN pg_type st ON st.oid = r.rngsubtype
WHERE r.rngtypid < 16384
ORDER BY t.typname;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE pcc_t CASCADE;
