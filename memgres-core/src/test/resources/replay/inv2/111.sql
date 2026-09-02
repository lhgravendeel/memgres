-- source: investigation-2026-08.md
-- finding: 111
-- title: The aggregate-call grammar is a fixed clause sequence: it matches DISTINCT with no ALL alternative, and reads FILTER strictly before WITHIN GROUP, so the standa
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(ALL v) FROM (VALUES (10),(20),(30)) t(v);
-- begin-expected
-- columns: sum:int8
-- row: 60
-- rowcount: 1
-- end-expected
SELECT sum(ALL v) FROM (VALUES (10),(20),(30)) t(v);
-- begin-expected
-- columns: string_agg:text
-- row: 10,20
-- rowcount: 1
-- end-expected
SELECT string_agg(ALL v::text, ',') FROM (VALUES (10),(20)) t(v);
-- begin-expected
-- columns: percentile_cont:float8
-- row: 25
-- rowcount: 1
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM (VALUES (10),(20),(30)) t(v);
-- begin-expected
-- columns: percentile_disc:int4
-- row: 20
-- rowcount: 1
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM (VALUES (10),(20),(30)) t(v);
-- begin-expected
-- columns: mode:int4
-- row: 20
-- rowcount: 1
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM (VALUES (10),(20),(30)) t(v);
-- begin-expected
-- columns: rank:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT rank(20) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM (VALUES (10),(20),(30)) t(v);
