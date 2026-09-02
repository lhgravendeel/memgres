-- source: review-2026-08.md
-- finding: Root cause 4: a relation whose target is not a plain Table falls out of the catalog builders
-- area: pg_dump fidelity
-- title: Root cause 4: a relation whose target is not a plain Table falls out of the catalog builders
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_b3" does not exist
-- end-expected-error
CREATE MATERIALIZED VIEW zz_vf2_m3 AS SELECT a, b FROM zz_vf2_b3;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_m3" does not exist
-- end-expected-error
CREATE UNIQUE INDEX zz_vf2_mi3 ON zz_vf2_m3 (a);
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_class WHERE relname = 'zz_vf2_mi3';
-- begin-expected
-- columns: relhasindex:bool
-- rowcount: 0
-- end-expected
SELECT relhasindex FROM pg_class WHERE relname = 'zz_vf2_m3';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pt (id integer NOT NULL, ts date NOT NULL) PARTITION BY RANGE (ts);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pta PARTITION OF zz_vf2_pt FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_pti ON zz_vf2_pt (id);
-- begin-expected
-- columns: relname:name | relispartition:bool
-- row: zz_vf2_pta_id_idx | t
-- rowcount: 1
-- end-expected
SELECT i.relname, i.relispartition FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
 JOIN pg_class t ON t.oid = x.indrelid WHERE t.relname = 'zz_vf2_pta';
-- begin-expected
-- columns: pg_get_indexdef:text
-- row: CREATE INDEX zz_vf2_pti ON ONLY public.zz_vf2_pt USING btree (id)
-- rowcount: 1
-- end-expected
SELECT pg_get_indexdef('zz_vf2_pti'::regclass);
