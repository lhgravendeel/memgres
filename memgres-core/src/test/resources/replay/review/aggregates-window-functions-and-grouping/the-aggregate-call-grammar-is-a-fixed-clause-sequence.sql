-- source: review-2026-08.md
-- finding: The aggregate-call grammar is a fixed clause sequence
-- area: Aggregates, window functions and grouping
-- title: The aggregate-call grammar is a fixed clause sequence
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
