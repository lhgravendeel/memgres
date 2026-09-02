-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Catalog builders and the wire layer, second pass
-- title: Unrelated singletons
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type pg_snapshot: "garbage"
-- end-expected-error
SELECT pg_snapshot_xmin('garbage');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type pg_snapshot: "garbage"
-- end-expected-error
SELECT pg_snapshot_xmax('garbage');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type pg_snapshot: ""
-- end-expected-error
SELECT pg_snapshot_xmin('');
-- begin-expected
-- columns: pg_stat_reset_shared:void
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_stat_reset_shared('checkpointer');
-- begin-expected
-- columns: pg_stat_reset_shared:void
-- row: 
-- rowcount: 1
-- end-expected
SELECT pg_stat_reset_shared('slru');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized reset target: "WAL"
-- end-expected-error
SELECT pg_stat_reset_shared('WAL');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_m (id int);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_vf2_m VALUES (1),(2),(3),(4);
-- begin-expected
-- columns: id:int4 | count:int8
-- row: 1 | 0
-- row: 2 | 0
-- row: 3 | 1
-- row: 4 | 2
-- rowcount: 4
-- end-expected
SELECT id, pg_catalog.count(*) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM zz_vf2_m ORDER BY id;
-- begin-expected
-- columns: id:int4 | row_number:int8
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 3
-- row: 4 | 4
-- rowcount: 4
-- end-expected
SELECT id, pg_catalog.row_number() OVER (ORDER BY id) FROM zz_vf2_m ORDER BY id;
-- begin-expected
-- columns: id:int4 | rank:int8
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 3
-- row: 4 | 4
-- rowcount: 4
-- end-expected
SELECT id, pg_catalog.rank() OVER (ORDER BY id) FROM zz_vf2_m ORDER BY id;
-- memgres run with -Duser.language=tr -Duser.country=TR
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_l2 (a interval MINUTE TO SECOND);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_am (j jsonb, t tsvector);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_am_gin ON zz_vf2_am USING GIN (j);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_am_gist ON zz_vf2_am USING GIST (t);
-- begin-expected
-- columns: relname:name | amname:name
-- row: zz_vf2_am_gin | gin
-- row: zz_vf2_am_gist | gist
-- rowcount: 2
-- end-expected
SELECT c.relname, a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam
 WHERE c.relname LIKE 'zz\_vf2\_am\_%' ORDER BY 1;
