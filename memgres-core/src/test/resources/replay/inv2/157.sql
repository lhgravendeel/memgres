-- source: investigation-2026-08.md
-- finding: 157
-- title: A partition or inheritance child is an independent table. Nothing is cloned or pushed down from the parent (triggers, indexes, ALTER-set NOT NULL, DEFAULTs, TRU
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_faf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.s := NEW.s || '!'; RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fa (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fa_1 PARTITION OF zz_vf_fa FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_fa_t BEFORE INSERT ON zz_vf_fa FOR EACH ROW EXECUTE FUNCTION zz_vf_faf();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_fa_1 VALUES (5, 'a');
-- begin-expected
-- columns: s:text
-- row: a!
-- rowcount: 1
-- end-expected
SELECT s FROM zz_vf_fa;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fh (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf_fh_idx ON zz_vf_fh (s);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fh_1 PARTITION OF zz_vf_fh FOR VALUES FROM (1) TO (10);
-- begin-expected
-- columns: n:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_index i JOIN pg_class c ON i.indrelid=c.oid WHERE c.relname='zz_vf_fh_1';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_gf (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_gf_1 PARTITION OF zz_vf_gf FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_gf_1 ALTER COLUMN s SET DEFAULT 'child';
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_gf ALTER COLUMN s SET DEFAULT 'parent';
-- begin-expected
-- columns: column_default:varchar
-- row: 'parent'::text
-- rowcount: 1
-- end-expected
SELECT column_default FROM information_schema.columns WHERE table_name='zz_vf_gf_1' AND column_name='s';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fop (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_foc () INHERITS (zz_vf_fop);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_foc VALUES (2);
-- begin-expected
-- ok: 0
-- end-expected
TRUNCATE zz_vf_fop;
-- begin-expected
-- columns: n:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM zz_vf_foc;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fi (i int) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fi_1 PARTITION OF zz_vf_fi FOR VALUES FROM (1) TO (10);
-- begin-expected
-- columns: attinhcount:int2 | attislocal:bool
-- row: 1 | f
-- rowcount: 1
-- end-expected
SELECT attinhcount, attislocal FROM pg_attribute WHERE attrelid='zz_vf_fi_1'::regclass AND attname='i';
-- begin-expected
-- columns: relhassubclass:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT relhassubclass FROM pg_class WHERE relname='zz_vf_fi';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pa (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pb (b int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pd () INHERITS (zz_vf_pa, zz_vf_pb);
-- begin-expected
-- columns: relname:name | inhseqno:int4
-- row: zz_vf_pa | 1
-- row: zz_vf_pb | 2
-- rowcount: 2
-- end-expected
SELECT p.relname, i.inhseqno FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_class p ON p.oid=i.inhparent WHERE c.relname='zz_vf_pd' ORDER BY i.inhseqno;
