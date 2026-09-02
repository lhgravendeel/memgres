-- source: review-2026-08.md
-- finding: Root cause 2: a partition or inheritance child is an independent table
-- area: DDL objects: indexes, sequences, types, functions, triggers, rules, views, partitioning and the catalogs that describe them
-- title: Root cause 2: a partition or inheritance child is an independent table
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
CREATE TABLE zz_vf_f4 (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_f4_1 PARTITION OF zz_vf_f4 FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_f4 ALTER COLUMN s SET NOT NULL;
-- begin-expected
-- columns: is_nullable:varchar
-- row: NO
-- rowcount: 1
-- end-expected
SELECT is_nullable FROM information_schema.columns WHERE table_name='zz_vf_f4_1' AND column_name='s';
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "s" of relation "zz_vf_f4_1" violates not-null constraint
-- end-expected-error
INSERT INTO zz_vf_f4_1 VALUES (2, NULL);
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "s" is marked NOT NULL in parent table
-- end-expected-error
ALTER TABLE zz_vf_f4_1 ALTER COLUMN s DROP NOT NULL;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ip (a int, s text NOT NULL);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ic () INHERITS (zz_vf_ip);
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited constraint "zz_vf_ip_s_not_null" of relation "zz_vf_ic"
-- end-expected-error
ALTER TABLE zz_vf_ic ALTER COLUMN s DROP NOT NULL;
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "s" of relation "zz_vf_ic" violates not-null constraint
-- end-expected-error
INSERT INTO zz_vf_ic VALUES (1, NULL);
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
SELECT p.relname, i.inhseqno FROM pg_inherits i
  JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_class p ON p.oid=i.inhparent
 WHERE c.relname='zz_vf_pd' ORDER BY i.inhseqno;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pt1 (i int) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pt1_a PARTITION OF zz_vf_pt1 FOR VALUES FROM (1) TO (10) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pt1_a1 PARTITION OF zz_vf_pt1_a FOR VALUES FROM (1) TO (5);
-- begin-expected
-- columns: r:text
-- row: zz_vf_pt1
-- rowcount: 1
-- end-expected
SELECT pg_partition_root('zz_vf_pt1_a1'::regclass)::text AS r;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_fj_1" does not exist
-- end-expected-error
SELECT relid::text FROM pg_partition_ancestors('zz_vf_fj_1'::regclass);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fk (i int);
-- begin-expected
-- columns: n:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_partition_tree('zz_vf_fk'::regclass);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fc (i int) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fc_d PARTITION OF zz_vf_fc DEFAULT;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_fc VALUES (50);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: updated partition constraint for default partition "zz_vf_fc_d" would be violated by some row
-- end-expected-error
CREATE TABLE zz_vf_fc_1 PARTITION OF zz_vf_fc FOR VALUES FROM (40) TO (60);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fd (i int, s text NOT NULL) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fdn (i int, s text);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "s" in child table "zz_vf_fdn" must be marked NOT NULL
-- end-expected-error
ALTER TABLE zz_vf_fd ATTACH PARTITION zz_vf_fdn FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_fdn VALUES (5, NULL);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fb (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fb_1 PARTITION OF zz_vf_fb FOR VALUES FROM (1) TO (10);
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop column "i" because it is part of the partition key of relation "zz_vf_fb"
-- end-expected-error
ALTER TABLE zz_vf_fb DROP COLUMN i;
