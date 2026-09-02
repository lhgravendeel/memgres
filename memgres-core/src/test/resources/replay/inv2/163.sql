-- source: investigation-2026-08.md
-- finding: 163
-- title: Unrelated singletons in this area
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
CREATE TABLE zz_vf_vv (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_trg() RETURNS trigger AS $$ begin raise notice '%', TG_RELNAME; return new; end $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_trg4 BEFORE INSERT ON zz_vf_vv FOR EACH ROW EXECUTE FUNCTION zz_vf_trg();
-- begin-expected
-- ok: 1
-- end-expected
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
CREATE TABLE zz_vf_t7 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_l7 (n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f7() RETURNS trigger AS $$
DECLARE n int; BEGIN SELECT count(*) INTO n FROM zz_vf_nt7; INSERT INTO zz_vf_l7 VALUES (n); RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_tg7 AFTER INSERT ON zz_vf_t7 REFERENCING NEW TABLE AS zz_vf_nt7 FOR EACH ROW EXECUTE FUNCTION zz_vf_f7();
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_t7 VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t2 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f2() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ct (a int);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: operator class int4_ops has no options
-- end-expected-error
CREATE INDEX zz_vf_i3 ON zz_vf_ct USING btree (a int4_ops(x = 1));
