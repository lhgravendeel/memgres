-- source: review-2026-08.md
-- finding: A row value becomes field-addressable only on the assignment path, and only for a type that names its fields
-- area: PL/pgSQL
-- title: A row value becomes field-addressable only on the assignment path, and only for a type that names its fields
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_nc AS (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_k2() RETURNS text AS $$ declare v zz_vf_nc := row(7,'q'); begin return v.a::text; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_k2:text
-- row: 7
-- rowcount: 1
-- end-expected
SELECT zz_vf_k2();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_nt(id int, nm text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_rowty() RETURNS text AS $$ declare v zz_vf_nt%rowtype := row(3,'x'); begin return v.nm; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_rowty:text
-- row: x
-- rowcount: 1
-- end-expected
SELECT zz_vf_rowty();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_k12() RETURNS text AS $$ declare r record; begin r := row(1,2); return r.f1::text; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_k12:text
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_vf_k12();
-- begin-expected-error
-- sqlstate: 55000
-- message-like: record "r" is not assigned yet
-- end-expected-error
DO $$ declare r record; begin if r.nm is null then null; end if; end $$;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_nt" already exists
-- end-expected-error
CREATE TABLE zz_vf_nt(id int, nm text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_nt VALUES (1,'a');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_k22() RETURNS text AS $$ declare r zz_vf_nt%rowtype; begin select * into r from zz_vf_nt limit 1; insert into zz_vf_nt values (r.*); return (select count(*)::text from zz_vf_nt); end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_k22:text
-- row: 2
-- rowcount: 1
-- end-expected
SELECT zz_vf_k22();
