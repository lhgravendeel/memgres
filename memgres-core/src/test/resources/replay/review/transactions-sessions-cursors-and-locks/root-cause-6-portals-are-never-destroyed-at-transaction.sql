-- source: review-2026-08.md
-- finding: Root cause 6: portals are never destroyed at transaction end
-- area: Transactions, sessions, cursors and locks
-- title: Root cause 6: portals are never destroyed at transaction end
-- extended protocol, autoCommit=false, fetchSize=1
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pt (i int);
-- begin-expected
-- ok: 5
-- end-expected
INSERT INTO zz_vf_pt SELECT g FROM generate_series(1,5) g;
-- SELECT i FROM zz_vf_pt ORDER BY i; read one row; COMMIT; read the next
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c1 (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_c1 VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_leak1() RETURNS int AS $$
DECLARE c CURSOR FOR SELECT id FROM zz_vf_c1 ORDER BY id; n int;
BEGIN OPEN c; FETCH c INTO n; RETURN n; END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_leak1:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_vf_leak1();
-- begin-expected
-- columns: zz_vf_leak1:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_vf_leak1();
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_cursors;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_z(id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_z VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_open() RETURNS refcursor AS $$
declare c refcursor := 'zz_vf_shared';
begin open c for select id from zz_vf_z order by id; return c; end $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_read(c refcursor) RETURNS text AS $$
declare v int; s text := '';
begin fetch c into v; s := v::text; fetch c into v; return s || ',' || v; end $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_both() RETURNS text AS $$
declare c refcursor; begin c := zz_vf_open(); return zz_vf_read(c); end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_both:text
-- row: 1,2
-- rowcount: 1
-- end-expected
SELECT zz_vf_both();
