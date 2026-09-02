-- source: investigation-2026-08.md
-- finding: 218
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_g_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.a := NEW.a + 10; RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g (a int, g int GENERATED ALWAYS AS (a * 2) STORED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_g_t BEFORE INSERT ON zz_g FOR EACH ROW EXECUTE FUNCTION zz_g_f();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_g (a) VALUES (1);
-- begin-expected
-- columns: a:int4 | g:int4
-- row: 11 | 22
-- rowcount: 1
-- end-expected
SELECT a, g FROM zz_g;
-- begin-expected
-- ok: 5
-- end-expected
CREATE TABLE zz_r2_ctas AS SELECT g AS id FROM generate_series(1,5) g;
-- begin-expected
-- columns: count:int8 | count:int8
-- row: 5 | 5
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT ctid::text), count(*) FROM zz_r2_ctas;
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM zz_r2_ctas WHERE ctid = (SELECT ctid FROM zz_r2_ctas WHERE id = 1);
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_r2_ctas;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE RULE zz_r_also AS ON INSERT TO zz_t DO ALSO INSERT INTO zz_log VALUES ('also', NEW.id);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE RULE zz_r_inst AS ON INSERT TO zz_t DO INSTEAD INSERT INTO zz_log VALUES ('instead', NEW.id);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
INSERT INTO zz_t VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_log" does not exist
-- end-expected-error
SELECT src, id FROM zz_log ORDER BY src;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rc_a" does not exist
-- end-expected-error
SELECT tableoid FROM zz_rc_a, zz_rc_b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rc_a" does not exist
-- end-expected-error
SELECT ctid FROM zz_rc_a, zz_rc_b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rc_a" does not exist
-- end-expected-error
SELECT xmin FROM zz_rc_a, zz_rc_b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rc_a" does not exist
-- end-expected-error
SELECT tableoid FROM zz_rc_a a1, zz_rc_a a2;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rc_a" does not exist
-- end-expected-error
SELECT cmin FROM zz_rc_a JOIN zz_rc_b ON true;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_c_s" does not exist
-- end-expected-error
DECLARE zz_c_x8 CURSOR FOR SELECT count(*) AS c FROM zz_c_s;
-- begin-expected
-- columns: is_scrollable:bool
-- rowcount: 0
-- end-expected
SELECT is_scrollable FROM pg_cursors WHERE name = 'zz_c_x8';
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_c_x8" does not exist
-- end-expected-error
FETCH 1 FROM zz_c_x8;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_c_x8" does not exist
-- end-expected-error
FETCH PRIOR FROM zz_c_x8;
-- begin-expected
-- columns: regexp_count:int4
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT regexp_count('abc', 'a', NULL);
-- begin-expected
-- columns: regexp_substr:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT regexp_substr('abc', 'a', NULL);
-- begin-expected
-- columns: has_function_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_function_privilege('sin(double precision)','EXECUTE')::text;
-- begin-expected
-- columns: has_function_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_function_privilege('gcd(integer,integer)','EXECUTE')::text;
-- begin-expected
-- columns: has_function_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_function_privilege('to_jsonb(anyelement)','EXECUTE')::text;
-- begin-expected
-- columns: count:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_proc WHERE proname IN ('gen_salt','crypt','digest','hmac');
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT to_regprocedure('gen_salt(text)') IS NOT NULL;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function gen_salt(unknown) does not exist
-- end-expected-error
SELECT length(gen_salt('md5')) > 0;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ix (a int, "my col" int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_i1 ON zz_ix (a) INCLUDE ("my col");
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_class WHERE relname='zz_i1';
-- begin-expected
-- ok: 0
-- end-expected
CREATE PUBLICATION zz_pub_p FOR ALL TABLES;
-- begin-expected
-- ok: 0
-- end-expected
ALTER PUBLICATION zz_pub_p SET (publish_generated_columns = 'stored');
