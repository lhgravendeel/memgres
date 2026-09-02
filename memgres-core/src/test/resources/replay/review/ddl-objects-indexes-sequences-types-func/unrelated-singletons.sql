-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: DDL objects: indexes, sequences, types, functions, triggers, rules, views, partitioning and the catalogs that describe them
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_f8 (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_f8_a PARTITION OF zz_vf_f8 (s DEFAULT 'c') FOR VALUES FROM (1) TO (10);
-- begin-expected
-- columns: b:text
-- row: FOR VALUES FROM (1) TO (10)
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(relpartbound, oid) AS b FROM pg_class WHERE relname='zz_vf_f8_a';
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_f8 VALUES (5, 'x');
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
LOCK TABLE zz_vf_t IN ACCESS EXCLUSIVE MODE;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_t;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_trg() RETURNS trigger AS $$ begin raise notice '%', TG_RELNAME; return new; end $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_vv" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_trg4 BEFORE INSERT ON zz_vf_vv FOR EACH ROW EXECUTE FUNCTION zz_vf_trg();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_vv" does not exist
-- end-expected-error
INSERT INTO zz_vf_vv VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rowt (a int, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_rowt VALUES (2, NULL);
-- begin-expected
-- columns: r:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT ROW(zz_vf_rowt.*) IS NOT NULL AS r FROM zz_vf_rowt;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_rowct AS (x int, y text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rowc (v zz_vf_rowct);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_rowc VALUES (ROW(NULL,NULL)::zz_vf_rowct);
-- begin-expected
-- columns: r:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT v IS NOT NULL AS r FROM zz_vf_rowc;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f7() RETURNS trigger AS $$
DECLARE n int; BEGIN SELECT count(*) INTO n FROM zz_vf_nt7; INSERT INTO zz_vf_l7 VALUES (n); RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_t7" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_tg7 AFTER INSERT ON zz_vf_t7 REFERENCING NEW TABLE AS zz_vf_nt7 FOR EACH ROW EXECUTE FUNCTION zz_vf_f7();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_t7" does not exist
-- end-expected-error
INSERT INTO zz_vf_t7 VALUES (1),(2);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_t2" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_tg2 AFTER INSERT ON zz_vf_t2 EXECUTE FUNCTION zz_vf_f2();
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
CREATE FUNCTION zz_vf_ca1() RETURNS int LANGUAGE sql BEGIN ATOMIC RETURN 42;
-- begin-expected
-- ok: 0
-- end-expected
END;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_ccomp AS (a int, b text);
-- begin-expected
-- columns: attname:name
-- row: a
-- row: b
-- rowcount: 2
-- end-expected
SELECT attname FROM pg_attribute WHERE attrelid = 'zz_vf_ccomp'::regclass AND attnum > 0 ORDER BY attnum;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_ccomp2 AS (a text COLLATE "C");
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ct" does not exist
-- end-expected-error
CREATE INDEX zz_vf_i3 ON zz_vf_ct USING btree (a int4_ops(x = 1));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fq (a int);
-- begin-expected
-- columns: n:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM zz_vf_fq*;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_trf() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.i < 151 THEN INSERT INTO zz_vf_tr VALUES (NEW.i + 1); END IF; RETURN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_tr" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_trg AFTER INSERT ON zz_vf_tr FOR EACH ROW EXECUTE FUNCTION zz_vf_trf();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_tr" does not exist
-- end-expected-error
INSERT INTO zz_vf_tr VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_tr" does not exist
-- end-expected-error
SELECT count(*)::int AS n FROM zz_vf_tr;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_etf() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'event trigger says no'; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE EVENT TRIGGER zz_vf_et ON ddl_command_start EXECUTE FUNCTION zz_vf_etf();
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
CREATE TABLE zz_vf_etx (i int);
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
CREATE FUNCTION zz_vf_cf11() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
ALTER FUNCTION zz_vf_cf11() COST -5;
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
CREATE SEQUENCE zz_vf_sq2 CACHE 4294967296;
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
CREATE TABLE zz_vf_fs (i int) PARTITION BY RANGE (i);
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
CREATE TABLE zz_vf_fs_1 PARTITION OF zz_vf_fs FOR VALUES FROM (100.5) TO (200.5);
-- begin-expected
-- columns: b:text
-- rowcount: 0
-- end-expected
SELECT pg_get_expr(relpartbound, oid) AS b FROM pg_class WHERE relname='zz_vf_fs_1';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_fs" does not exist
-- end-expected-error
INSERT INTO zz_vf_fs VALUES (100);
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
CREATE OPERATOR #%! (LEFTARG = int, RIGHTARG = int, FUNCTION = zz_vf_dop2);
-- twice
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
DROP OPERATOR #%# (int, int);
-- begin-expected-error
-- sqlstate: P0001
-- message-like: event trigger says no
-- end-expected-error
CREATE OPERATOR #%@ (LEFTARG = int, FUNCTION = zz_vf_dop2);
