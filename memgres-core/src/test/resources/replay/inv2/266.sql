-- source: investigation-2026-08.md
-- finding: 266
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rt(id int, nm text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_rt VALUES (1,'a'),(2,'b');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_k19() RETURNS text AS $$ declare c refcursor; v text; begin open c for execute 'select nm from zz_vf_rt where id = $1' using 1; fetch c into v; close c; return v; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_k19:text
-- row: a
-- rowcount: 1
-- end-expected
SELECT zz_vf_k19();
-- begin-expected
-- ok: 0
-- end-expected
DO $$ declare "quoted name" int := 3; begin if "quoted name" <> 3 then raise exception 'bad'; end if; end $$;
-- begin-expected
-- ok: 0
-- end-expected
DO $$ declare x text collate "C" := 'a'; begin null; end $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
DO $$ declare x int collate "C"; begin null; end $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_x5() RETURNS text AS $$ declare v char(5); begin v := 'ab'; return '[' || v || ']'; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_x5:text
-- row: [ab]
-- rowcount: 1
-- end-expected
SELECT zz_vf_x5();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ft(id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ft VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_x9() RETURNS text AS $$ declare n int; begin select id from zz_vf_ft where id = 1; get diagnostics n = row_count; return n::text; end $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: query has no destination for result data
-- end-expected-error
SELECT zz_vf_x9();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_bigparam() RETURNS int AS $$ DECLARE n int; BEGIN EXECUTE 'SELECT $99999999999' INTO n USING 1; RETURN n; END $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: parameter number too large at or near "$99999999999"
-- end-expected-error
SELECT zz_vf_bigparam();
