-- source: investigation-2026-08.md
-- finding: 125
-- title: LATERAL scope is computed by scanning the top-level FROM list only; the scan never descends into a JoinFrom and the join executor treats only join.right() as la
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
