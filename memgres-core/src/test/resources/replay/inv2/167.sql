-- source: investigation-2026-08.md
-- finding: 167
-- title: every pg_aggregate row is the same hardcoded constant: aggkind 'n', aggnumdirectargs 0, RegprocValue(0,"-") for all eight support functions and no init value
-- begin-expected
-- columns: count:int4
-- row: 8
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_aggregate a JOIN pg_proc p ON p.oid=a.aggfnoid
 WHERE p.proname IN ('percentile_cont','mode','rank','dense_rank','cume_dist');
-- begin-expected
-- columns: aggtransfn:text | agginitval:text
-- row: int8inc | 0
-- row: int8inc_any | 0
-- rowcount: 2
-- end-expected
SELECT a.aggtransfn::text, a.agginitval FROM pg_aggregate a JOIN pg_proc p ON p.oid=a.aggfnoid WHERE p.proname='count' ORDER BY 1,2;
