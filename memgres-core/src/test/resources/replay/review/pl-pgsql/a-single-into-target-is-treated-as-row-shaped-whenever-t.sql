-- source: review-2026-08.md
-- finding: A single INTO target is treated as row-shaped whenever the query returns more than one column
-- area: PL/pgSQL
-- title: A single INTO target is treated as row-shaped whenever the query returns more than one column
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rt(id int, nm text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_rt VALUES (1,'a');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_k16() RETURNS text AS $$ declare i int; begin select id, nm into i from zz_vf_rt; return i::text; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_k16:text
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_vf_k16();
