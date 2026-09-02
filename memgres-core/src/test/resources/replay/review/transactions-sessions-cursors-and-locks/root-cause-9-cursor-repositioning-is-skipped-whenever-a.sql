-- source: review-2026-08.md
-- finding: Root cause 9: cursor repositioning is skipped whenever a counted fetch returned something
-- area: Transactions, sessions, cursors and locks
-- title: Root cause 9: cursor repositioning is skipped whenever a counted fetch returned something
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c (i int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_c VALUES (1),(2),(3);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_vf_x6 SCROLL CURSOR FOR SELECT i FROM zz_vf_c ORDER BY i;
-- begin-expected
-- columns: i:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
FETCH 2 FROM zz_vf_x6;
-- begin-expected
-- columns: i:int4
-- row: 1
-- rowcount: 1
-- end-expected
FETCH BACKWARD 2 FROM zz_vf_x6;
-- begin-expected
-- columns: i:int4
-- row: 1
-- row: 2
-- row: 3
-- rowcount: 3
-- end-expected
FETCH ALL FROM zz_vf_x6;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_z6(id int);
-- begin-expected
-- ok: 6
-- end-expected
INSERT INTO zz_vf_z6 SELECT g FROM generate_series(1,6) g;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_y2() RETURNS text AS $$
declare c scroll cursor for select id from zz_vf_z6 order by id; v int;
begin open c; move all from c; fetch backward from c into v; close c; return v::text; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_y2:text
-- row: 6
-- rowcount: 1
-- end-expected
SELECT zz_vf_y2();
