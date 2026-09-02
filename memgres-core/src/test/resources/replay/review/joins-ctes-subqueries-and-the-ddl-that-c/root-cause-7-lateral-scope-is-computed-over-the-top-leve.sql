-- source: review-2026-08.md
-- finding: Root cause 7: LATERAL scope is computed over the top-level FROM list only
-- area: Joins, CTEs, subqueries — and the DDL that came with them
-- title: Root cause 7: LATERAL scope is computed over the top-level FROM list only
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_la (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lb (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_la VALUES (1),(2);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_lb VALUES (1),(2);
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_la a JOIN (zz_lb b JOIN LATERAL (SELECT a.id AS k) s ON true) ON true;
