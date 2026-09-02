-- source: investigation.md
-- finding: 101
-- title: Array types are not linked back from their element types ⚠️
-- every non-pseudo type must have a matching array type, cross-linked both ways
-- begin-expected
-- columns: typname:name
-- row: pg_brin_minmax_multi_summary
-- row: pg_ndistinct
-- row: pg_dependencies
-- row: pg_node_tree
-- row: pg_brin_bloom_summary
-- row: pg_mcv_list
-- rowcount: 6
-- end-expected
SELECT t1.typname FROM pg_type t1 WHERE t1.typtype <> 'p' AND t1.typname NOT LIKE '\_%'
  AND NOT EXISTS (SELECT 1 FROM pg_type t2 WHERE t2.typname = '_' || t1.typname
                  AND t2.typelem = t1.oid AND t1.typarray = t2.oid);
--   PG: 6 rows   memgres: 51 rows
--   offenders include: int2, int8, float4, float8, numeric, varchar, bpchar, name;
