CREATE TABLE zd_p1 (a integer PRIMARY KEY, b integer UNIQUE);
CREATE TABLE zd_d1 (id integer, d_expr integer DEFAULT (2 + 3) * 4, d_expr2 integer DEFAULT 2 * (3 + 4));
-- begin-expected
-- columns: n | e
-- row: d_expr | ((2 + 3) * 4)
-- row: d_expr2 | (2 * (3 + 4))
-- end-expected
SELECT a.attname::text AS n, pg_get_expr(d.adbin, d.adrelid) AS e FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum JOIN pg_class c ON c.oid = d.adrelid WHERE c.relname = 'zd_d1' ORDER BY a.attnum;
CREATE TABLE zd_c1 (x integer, y integer, z text, CONSTRAINT zd_ckb CHECK (z IS NULL OR length(z) > 2) NO INHERIT, CONSTRAINT zd_fka FOREIGN KEY (x) REFERENCES zd_p1(a) DEFERRABLE INITIALLY DEFERRED, CONSTRAINT zd_fkb FOREIGN KEY (y) REFERENCES zd_p1(b) MATCH FULL ON DELETE SET NULL DEFERRABLE);
-- begin-expected
-- columns: n | d
-- row: zd_ckb | CHECK (((z IS NULL) OR (length(z) > 2))) NO INHERIT
-- row: zd_fka | FOREIGN KEY (x) REFERENCES zd_p1(a) DEFERRABLE INITIALLY DEFERRED
-- row: zd_fkb | FOREIGN KEY (y) REFERENCES zd_p1(b) MATCH FULL ON DELETE SET NULL DEFERRABLE
-- end-expected
SELECT conname::text AS n, pg_get_constraintdef(oid) AS d FROM pg_constraint WHERE conrelid = 'zd_c1'::regclass ORDER BY conname;
CREATE SCHEMA zd_s3;
CREATE TABLE zd_s3.zd_b3 (a int, b int, d int);
CREATE VIEW zd_s3.zd_v3 AS SELECT a, b FROM zd_s3.zd_b3 WHERE d > 0;
-- begin-expected
-- columns: d
-- row:  SELECT a,     b    FROM zd_s3.zd_b3   WHERE d > 0;
-- end-expected
SELECT replace(pg_get_viewdef('zd_s3.zd_v3'::regclass, true), chr(10), ' ') AS d;
CREATE SCHEMA zd_s6;
CREATE TABLE zd_s6.zd_tt6 (id int);
CREATE FUNCTION zd_s6.zd_tf6() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE CONSTRAINT TRIGGER zd_tg6c AFTER INSERT ON zd_s6.zd_tt6 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zd_s6.zd_tf6();
-- begin-expected
-- columns: d | c
-- row: CREATE CONSTRAINT TRIGGER zd_tg6c AFTER INSERT ON zd_s6.zd_tt6 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zd_s6.zd_tf6() | true
-- end-expected
SELECT pg_get_triggerdef(oid, true) AS d, (tgconstraint <> 0)::text AS c FROM pg_trigger WHERE tgname = 'zd_tg6c';
CREATE FUNCTION zd_f7() RETURNS integer LANGUAGE sql VOLATILE SECURITY DEFINER LEAKPROOF PARALLEL SAFE COST 500 SET search_path TO public, pg_temp SET work_mem TO '4MB' AS $$ SELECT 1 $$;
-- begin-expected
-- columns: d
-- row: CREATE OR REPLACE FUNCTION public.zd_f7()  RETURNS integer  LANGUAGE sql  PARALLEL SAFE SECURITY DEFINER LEAKPROOF COST 500  SET search_path TO 'public', 'pg_temp'  SET work_mem TO '4MB' AS $function$ SELECT 1 $function$ 
-- end-expected
SELECT replace(pg_get_functiondef('zd_f7()'::regprocedure), chr(10), ' ') AS d;
CREATE TYPE zd_ct1 AS (street text, city varchar(40), zip char(6), amt numeric(8,2));
-- begin-expected
-- columns: n | f
-- row: street | text
-- row: city | character varying(40)
-- row: zip | character(6)
-- row: amt | numeric(8,2)
-- end-expected
SELECT a.attname::text AS n, format_type(a.atttypid, a.atttypmod) AS f FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_type t ON t.typrelid = c.oid WHERE t.typname = 'zd_ct1' AND a.attnum > 0 ORDER BY a.attnum;
CREATE DOMAIN zd_dv1 AS varchar(20);
CREATE DOMAIN zd_dn1 AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);
-- begin-expected
-- columns: f
-- row: character varying(20)
-- end-expected
SELECT format_type(typbasetype, typtypmod) AS f FROM pg_type WHERE typname = 'zd_dv1';
-- begin-expected
-- columns: l | bv | st | o
-- row: 4 | true | p | int4out
-- end-expected
SELECT typlen::text AS l, typbyval::text AS bv, typstorage::text AS st, typoutput::text AS o FROM pg_type WHERE typname = 'zd_dn1';
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT pg_get_expr(typdefaultbin, 0) AS d FROM pg_type WHERE typname = 'zd_dn1';
CREATE TABLE zd_t1 (c_arrn numeric(6,2)[], c_arrv varchar(9)[]);
-- begin-expected
-- columns: n | m | f
-- row: c_arrn | 393222 | numeric(6,2)[]
-- row: c_arrv | 13 | character varying(9)[]
-- end-expected
SELECT a.attname::text AS n, a.atttypmod::text AS m, format_type(a.atttypid, a.atttypmod) AS f FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'zd_t1' AND a.attnum > 0 ORDER BY a.attnum;
CREATE MATERIALIZED VIEW zd_m3 AS SELECT a, b FROM zd_s3.zd_b3;
CREATE UNIQUE INDEX zd_mi3 ON zd_m3 (a);
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname = 'zd_mi3';
-- begin-expected
-- columns: h
-- row: true
-- end-expected
SELECT relhasindex::text AS h FROM pg_class WHERE relname = 'zd_m3';
CREATE TABLE zd_pt (id integer NOT NULL, ts date NOT NULL) PARTITION BY RANGE (ts);
CREATE TABLE zd_pta PARTITION OF zd_pt FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE INDEX zd_pti ON zd_pt (id);
-- begin-expected
-- columns: n | p
-- row: zd_pta_id_idx | true
-- end-expected
SELECT i.relname::text AS n, i.relispartition::text AS p FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_class t ON t.oid = x.indrelid WHERE t.relname = 'zd_pta';
-- begin-expected
-- columns: d
-- row: CREATE INDEX zd_pti ON ONLY public.zd_pt USING btree (id)
-- end-expected
SELECT pg_get_indexdef('zd_pti'::regclass) AS d;
-- begin-expected
-- columns: a | k
-- row: 0 | p
-- end-expected
SELECT relam::text AS a, relkind::text AS k FROM pg_class WHERE relname = 'zd_pt';
CREATE TABLE zd_t7 (a integer, b integer, c text);
-- begin-expected
-- columns: h
-- row: true
-- end-expected
SELECT (reltoastrelid <> 0)::text AS h FROM pg_class WHERE relname = 'zd_t7';
CREATE TABLE zd_s6.zd_t6 (id integer, n integer);
ALTER TABLE zd_s6.zd_t6 ENABLE ROW LEVEL SECURITY;
CREATE POLICY zd_po6 ON zd_s6.zd_t6 FOR SELECT USING (n > 0);
-- begin-expected
-- columns: n
-- row: zd_po6
-- end-expected
SELECT polname::text AS n FROM pg_policy WHERE polrelid = 'zd_s6.zd_t6'::regclass;
DROP TABLE zd_pt CASCADE;
DROP MATERIALIZED VIEW zd_m3;
DROP TABLE zd_t1, zd_t7, zd_c1, zd_d1, zd_p1 CASCADE;
DROP DOMAIN zd_dv1, zd_dn1;
DROP TYPE zd_ct1;
DROP FUNCTION zd_f7();
DROP SCHEMA zd_s3 CASCADE;
DROP SCHEMA zd_s6 CASCADE;
