-- source: investigation-2026-08.md
-- finding: 140
-- title: HASH partition routing uses Thomas Wang's hash32shift with Math.abs for numbers and Java's String.hashCode for everything else, instead of PostgreSQL's hash_uin
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ha (i int) PARTITION BY HASH (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ha_0 PARTITION OF zz_vf_ha FOR VALUES WITH (MODULUS 4, REMAINDER 0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ha_1 PARTITION OF zz_vf_ha FOR VALUES WITH (MODULUS 4, REMAINDER 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ha_2 PARTITION OF zz_vf_ha FOR VALUES WITH (MODULUS 4, REMAINDER 2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ha_3 PARTITION OF zz_vf_ha FOR VALUES WITH (MODULUS 4, REMAINDER 3);
-- begin-expected
-- ok: 12
-- end-expected
INSERT INTO zz_vf_ha SELECT g FROM generate_series(1,12) g;
-- begin-expected
-- columns: i:int4
-- row: 1
-- row: 12
-- rowcount: 2
-- end-expected
SELECT i FROM zz_vf_ha_0 ORDER BY i;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_shp (i int) PARTITION BY HASH (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_shp_0 PARTITION OF zz_vf_shp FOR VALUES WITH (MODULUS 2, REMAINDER 0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_shp_1 PARTITION OF zz_vf_shp FOR VALUES WITH (MODULUS 2, REMAINDER 1);
-- begin-expected
-- columns: satisfies_hash_partition:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT satisfies_hash_partition('zz_vf_shp'::regclass, 2, 0, 1);
