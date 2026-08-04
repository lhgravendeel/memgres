-- ============================================================================
-- Feature Comparison: pg_cast, pg_operator, pg_opclass, pg_opfamily, pg_amop,
--                     pg_amproc, pg_collation, pg_am -- their content
-- Target: PostgreSQL 18 vs Memgres
--
-- Every case here names the row it is about. A count of a catalogue cannot be
-- compared: the reference server carries contrib extensions and collations
-- imported from the host's locale list that no memgres instance has, so a
-- count would differ for reasons that are not memgres's doing. What can be
-- compared, and is what a tool actually reads, is a named row and its named
-- columns.
--
-- What was wrong, measured before the fix:
--   * pg_opclass gave int4_ops the OID 403, which is btree's own pg_am OID,
--     while index creation writes 1978 into pg_index.indclass for an integer
--     column -- so every integer index and every integer primary key named a
--     pg_opclass row that was not there;
--   * the gist, spgist and brin operator classes were absent altogether, and
--     a gin index on jsonb_path_ops dangled the same way;
--   * pg_amop held ten rows for two families, so seventy-two of memgres's own
--     operator families claimed no operators at all;
--   * pg_operator numbered all 799 built-in operators above 16384, the range
--     PostgreSQL reserves for objects somebody created, and said
--     oprcanmerge/oprcanhash were false for =(int4,int4);
--   * pg_collation offered an en_US row PostgreSQL has nowhere in that shape,
--     and was missing pg_unicode_fast.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS cocc_t CASCADE;

CREATE TABLE cocc_t (i int, s text, d date, u uuid, j jsonb, PRIMARY KEY (i));
CREATE INDEX cocc_bi ON cocc_t (i);
CREATE INDEX cocc_bs ON cocc_t (s);
CREATE INDEX cocc_bd ON cocc_t (d);
CREATE INDEX cocc_bu ON cocc_t (u);
CREATE INDEX cocc_hi ON cocc_t USING hash (i);
CREATE INDEX cocc_gj ON cocc_t USING gin (j jsonb_path_ops);
CREATE INDEX cocc_pat ON cocc_t (s text_pattern_ops);

-- ============================================================================
-- pg_opclass: the classes, at the OIDs PostgreSQL gives them
-- ============================================================================

-- int4_ops has to be findable at 1978: that is the number index creation puts
-- in pg_index.indclass for an integer column.
-- begin-expected
-- columns: oid|opcname|opcintype|opckeytype|opcdefault|opcmethod
-- row: 1978|int4_ops|23|0|t|403
-- end-expected
SELECT oid, opcname, opcintype, opckeytype, opcdefault, opcmethod
FROM pg_opclass WHERE opcname = 'int4_ops' AND opcmethod = 403;

-- A shipped object is numbered below FirstNormalObjectId; anything at or above
-- it is something somebody created.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_opclass
WHERE opcname = 'int4_ops' AND opcmethod = 403 AND oid >= 16384;

-- One named class per access method, with the family and input type PG gives it.
-- begin-expected
-- columns: amname|opcname|intype|opcdefault|opfname
-- row: brin|box_inclusion_ops|box|t|box_inclusion_ops
-- row: brin|int4_minmax_ops|integer|t|integer_minmax_ops
-- row: btree|text_pattern_ops|text|f|text_pattern_ops
-- row: gin|jsonb_path_ops|jsonb|f|jsonb_path_ops
-- row: hash|text_pattern_ops|text|f|text_pattern_ops
-- row: spgist|quad_point_ops|point|t|quad_point_ops
-- end-expected
SELECT a.amname, o.opcname, format_type(o.opcintype, NULL) AS intype, o.opcdefault, f.opfname
FROM pg_opclass o
JOIN pg_am a ON a.oid = o.opcmethod
JOIN pg_opfamily f ON f.oid = o.opcfamily
WHERE o.opcname IN ('text_pattern_ops', 'jsonb_path_ops', 'quad_point_ops',
                    'int4_minmax_ops', 'box_inclusion_ops')
ORDER BY 1, 2;

-- psql's \dAc brin int4 returns exactly these three.
-- begin-expected
-- columns: opcname
-- row: int4_bloom_ops
-- row: int4_minmax_multi_ops
-- row: int4_minmax_ops
-- end-expected
SELECT o.opcname FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod
WHERE a.amname = 'brin' AND o.opcintype = 23 ORDER BY 1;

-- The seven spgist classes PostgreSQL ships, by name.
-- begin-expected
-- columns: n
-- row: 7
-- end-expected
SELECT count(*) AS n FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod
WHERE a.amname = 'spgist' AND o.opcname IN ('quad_point_ops', 'kd_point_ops', 'text_ops',
                                            'poly_ops', 'box_ops', 'range_ops', 'inet_ops');

-- A class with a storage type says which type it stores; the reader of a gin
-- index needs it to know what the entries are.
-- begin-expected
-- columns: opcname|opckeytype
-- row: array_ops|anyelement
-- row: jsonb_ops|text
-- end-expected
SELECT o.opcname, o.opckeytype::regtype::text AS opckeytype
FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod
WHERE a.amname = 'gin' AND o.opcname IN ('jsonb_ops', 'array_ops') ORDER BY 1;

-- varchar_ops is not the default btree class for anything, and it is keyed on
-- text; name_ops is keyed on name and is the default.
-- begin-expected
-- columns: opcname|opcdefault|intype
-- row: name_ops|t|name
-- row: record_image_ops|f|record
-- row: varchar_ops|f|text
-- end-expected
SELECT o.opcname, o.opcdefault, format_type(o.opcintype, NULL) AS intype
FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod
WHERE a.amname = 'btree' AND o.opcname IN ('varchar_ops', 'name_ops', 'record_image_ops')
ORDER BY 1;

-- PostgreSQL's own rule: one default class per (access method, input type).
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM (SELECT opcmethod, opcintype FROM pg_opclass
                           WHERE opcdefault GROUP BY opcmethod, opcintype HAVING count(*) > 1) d;

-- ============================================================================
-- pg_amop: which operator answers which strategy
-- ============================================================================

-- The pattern family answers with the pattern operators, not the ordinary ones.
-- begin-expected
-- columns: amopstrategy|oprname
-- row: 1|~<~
-- row: 2|~<=~
-- row: 3|=
-- row: 4|~>=~
-- row: 5|~>~
-- end-expected
SELECT p.amopstrategy, op.oprname FROM pg_amop p
JOIN pg_opfamily f ON f.oid = p.amopfamily
JOIN pg_am a ON a.oid = p.amopmethod
JOIN pg_operator op ON op.oid = p.amopopr
WHERE f.opfname = 'text_pattern_ops' AND a.amname = 'btree' AND p.amoplefttype = 25
ORDER BY 1;

-- btree's five search strategies over integer.
-- begin-expected
-- columns: amopstrategy|oprname|amoppurpose
-- row: 1|<|s
-- row: 2|<=|s
-- row: 3|=|s
-- row: 4|>=|s
-- row: 5|>|s
-- end-expected
SELECT p.amopstrategy, op.oprname, p.amoppurpose FROM pg_amop p
JOIN pg_opfamily f ON f.oid = p.amopfamily
JOIN pg_am a ON a.oid = p.amopmethod
JOIN pg_operator op ON op.oid = p.amopopr
WHERE f.opfname = 'integer_ops' AND a.amname = 'btree'
  AND p.amoplefttype = 23 AND p.amoprighttype = 23
ORDER BY 1;

-- gin over jsonb answers containment and the key tests, not comparison.
-- begin-expected
-- columns: amopstrategy|oprname
-- row: 7|@>
-- row: 9|?
-- row: 10|?|
-- row: 11|?&
-- row: 15|@?
-- row: 16|@@
-- end-expected
SELECT p.amopstrategy, op.oprname FROM pg_amop p
JOIN pg_opfamily f ON f.oid = p.amopfamily
JOIN pg_am a ON a.oid = p.amopmethod
JOIN pg_operator op ON op.oid = p.amopopr
WHERE f.opfname = 'jsonb_ops' AND a.amname = 'gin' ORDER BY 1;

-- An ordering operator carries purpose 'o' and names the family its distances
-- sort in; a nearest-neighbour gist query is planned off exactly this row.
-- begin-expected
-- columns: opfname|amname|amopstrategy|oprname|sortfam
-- row: point_ops|gist|15|<->|float_ops
-- end-expected
SELECT f.opfname, a.amname, p.amopstrategy, op.oprname, sf.opfname AS sortfam
FROM pg_amop p
JOIN pg_opfamily f ON f.oid = p.amopfamily
JOIN pg_am a ON a.oid = p.amopmethod
JOIN pg_operator op ON op.oid = p.amopopr
JOIN pg_opfamily sf ON sf.oid = p.amopsortfamily
WHERE p.amoppurpose = 'o' AND f.opfname = 'point_ops' AND a.amname = 'gist';

-- Named families that must have operators behind them.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_opfamily f JOIN pg_am a ON a.oid = f.opfmethod
WHERE f.opfname IN ('integer_ops', 'text_ops', 'datetime_ops', 'float_ops', 'numeric_ops',
                    'uuid_ops', 'bool_ops', 'network_ops', 'jsonb_ops', 'array_ops',
                    'text_pattern_ops', 'integer_minmax_ops', 'quad_point_ops',
                    'box_inclusion_ops')
  AND NOT EXISTS (SELECT 1 FROM pg_amop x WHERE x.amopfamily = f.oid);

-- ============================================================================
-- pg_operator
-- ============================================================================

-- Equality over integer is int4eq, admits a merge join and a hash join, and
-- lives at PostgreSQL's own OID 96.
-- begin-expected
-- columns: oid|oprcanmerge|oprcanhash|oprcode
-- row: 96|t|t|int4eq
-- end-expected
SELECT oid, oprcanmerge, oprcanhash, oprcode::text AS oprcode
FROM pg_operator WHERE oprname = '=' AND oprleft = 23 AND oprright = 23;

-- Five more built-ins at the numbers PostgreSQL gives them, with the merge and
-- hash flags a planner reads.
-- begin-expected
-- columns: oprname|oid|oprcanmerge|oprcanhash
-- row: <|97|f|f
-- row: =|98|t|t
-- row: +|551|f|f
-- row: |||654|f|f
-- row: =|2972|t|t
-- end-expected
SELECT oprname, oid, oprcanmerge, oprcanhash FROM pg_operator
WHERE (oprname = '<' AND oprleft = 23 AND oprright = 23)
   OR (oprname = '=' AND oprleft = 25 AND oprright = 25)
   OR (oprname = '+' AND oprleft = 23 AND oprright = 23)
   OR (oprname = '||' AND oprleft = 25 AND oprright = 25)
   OR (oprname = '=' AND oprleft = 2950 AND oprright = 2950)
ORDER BY 2;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_operator
WHERE oprname = '=' AND oprleft = 23 AND oprright = 23 AND oid >= 16384;

-- ============================================================================
-- pg_cast: a cast is referred to by OID and by nothing else
-- ============================================================================

-- begin-expected
-- columns: oid|castcontext|castmethod
-- row: 10035|e|f
-- end-expected
SELECT oid, castcontext, castmethod FROM pg_cast WHERE castsource = 16 AND casttarget = 23;

-- begin-expected
-- columns: castcontext|castmethod|oid
-- row: i|b|10125
-- end-expected
SELECT castcontext, castmethod, oid FROM pg_cast WHERE castsource = 25 AND casttarget = 1042;

-- ============================================================================
-- pg_collation and pg_am
-- ============================================================================

-- The seven collations PG 18 compiles in, with the OIDs it pins them at.
-- begin-expected
-- columns: collname|collprovider|collisdeterministic|collencoding|colllocale
-- row: C|c|t|-1|NULL
-- row: POSIX|c|t|-1|NULL
-- row: default|d|t|-1|NULL
-- row: pg_c_utf8|b|t|6|C.UTF-8
-- row: pg_unicode_fast|b|t|6|PG_UNICODE_FAST
-- row: ucs_basic|b|t|6|C
-- row: unicode|i|t|-1|und
-- end-expected
SELECT collname, collprovider, collisdeterministic, collencoding, colllocale
FROM pg_collation
WHERE collname IN ('pg_unicode_fast', 'pg_c_utf8', 'ucs_basic', 'unicode', 'C', 'POSIX', 'default')
ORDER BY collname;

-- begin-expected
-- columns: collname|oid
-- row: C|950
-- row: POSIX|951
-- row: default|100
-- row: pg_c_utf8|811
-- row: pg_unicode_fast|6411
-- row: ucs_basic|962
-- row: unicode|963
-- end-expected
SELECT collname, oid FROM pg_collation
WHERE collname IN ('default', 'C', 'POSIX', 'ucs_basic', 'pg_c_utf8', 'unicode', 'pg_unicode_fast')
ORDER BY 1;

-- There is no en_US row as an ICU collation at UTF8. PostgreSQL imports en_US
-- only on a host whose locale list has it, and then as libc at that locale's
-- own encoding.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_collation
WHERE collname = 'en_US' AND collencoding = 6 AND collprovider = 'i';

-- begin-expected
-- columns: amname|amtype
-- row: heap|t
-- row: btree|i
-- row: hash|i
-- row: gist|i
-- row: gin|i
-- row: brin|i
-- row: spgist|i
-- end-expected
SELECT amname, amtype FROM pg_am ORDER BY oid;

-- ============================================================================
-- Referential integrity: nothing may name a row that is not there
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_opclass o
WHERE NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = o.opcfamily)
   OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.opcintype)
   OR NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = o.opcmethod)
   OR (o.opckeytype <> 0 AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.opckeytype));

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_amop p
WHERE NOT EXISTS (SELECT 1 FROM pg_operator o WHERE o.oid = p.amopopr)
   OR NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = p.amopfamily)
   OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = p.amoplefttype)
   OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = p.amoprighttype)
   OR NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = p.amopmethod)
   OR (p.amopsortfamily <> 0
       AND NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = p.amopsortfamily));

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_amproc p
WHERE NOT EXISTS (SELECT 1 FROM pg_proc r WHERE r.oid = p.amproc);

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_cast c
WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.castsource)
   OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.casttarget)
   OR (c.castfunc <> 0 AND NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = c.castfunc));

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_operator o
WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprresult)
   OR NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = o.oprcode)
   OR (o.oprleft <> 0 AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprleft))
   OR (o.oprright <> 0 AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprright))
   OR (o.oprcom <> 0 AND NOT EXISTS (SELECT 1 FROM pg_operator x WHERE x.oid = o.oprcom))
   OR (o.oprnegate <> 0 AND NOT EXISTS (SELECT 1 FROM pg_operator x WHERE x.oid = o.oprnegate));

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_am a
WHERE NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = a.amhandler);

-- Every index names an operator class that exists. Before the fix an integer
-- primary key, a hash index on integer and a gin index on jsonb_path_ops all
-- pointed pg_index at no row at all.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_index i
WHERE NOT EXISTS (SELECT 1 FROM pg_opclass o WHERE o.oid = i.indclass[0]);

-- begin-expected
-- columns: relname|opcname
-- row: cocc_bd|date_ops
-- row: cocc_bi|int4_ops
-- row: cocc_bs|text_ops
-- row: cocc_bu|uuid_ops
-- row: cocc_gj|jsonb_path_ops
-- row: cocc_hi|int4_ops
-- row: cocc_pat|text_pattern_ops
-- row: cocc_t_pkey|int4_ops
-- end-expected
SELECT c.relname, coalesce(o.opcname, '<DANGLING>') AS opcname
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
LEFT JOIN pg_opclass o ON o.oid = i.indclass[0]
WHERE c.relname LIKE 'cocc_%'
ORDER BY 1;

-- No catalogue may hand out an OID twice: every join over these reads by OID.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM (SELECT oid FROM pg_opclass GROUP BY oid HAVING count(*) > 1) d;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM (SELECT oid FROM pg_amop GROUP BY oid HAVING count(*) > 1) d;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM (SELECT oid FROM pg_operator GROUP BY oid HAVING count(*) > 1) d;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM (SELECT oid FROM pg_cast GROUP BY oid HAVING count(*) > 1) d;

-- ============================================================================
-- Teardown
-- ============================================================================

DROP TABLE IF EXISTS cocc_t CASCADE;
