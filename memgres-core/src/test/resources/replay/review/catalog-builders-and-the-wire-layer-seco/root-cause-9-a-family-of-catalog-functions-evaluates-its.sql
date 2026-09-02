-- source: review-2026-08.md
-- finding: Root cause 9: a family of catalog functions evaluates its argument for its own errors and then returns a constant
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 9: a family of catalog functions evaluates its argument for its own errors and then returns a constant
-- begin-expected-error
-- sqlstate: 3D000
-- message-like: database "zz_vf2_nodb" does not exist
-- end-expected-error
SELECT pg_database_size('zz_vf2_nodb');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_notable" does not exist
-- end-expected-error
SELECT pg_relation_size('zz_vf2_notable');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_notable" does not exist
-- end-expected-error
SELECT pg_total_relation_size('zz_vf2_notable');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_notable" does not exist
-- end-expected-error
SELECT pg_table_size('zz_vf2_notable');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_notable" does not exist
-- end-expected-error
SELECT pg_indexes_size('zz_vf2_notable');
-- begin-expected-error
-- sqlstate: 58P01
-- message-like: could not stat file "pg_tblspc/999999": No such file or directory
-- end-expected-error
SELECT pg_tablespace_location(999999::oid);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: replication slot "zz_vf2_noslot" does not exist
-- end-expected-error
SELECT pg_drop_replication_slot('zz_vf2_noslot');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(405,'can_order');
-- hash
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(405,'can_unique');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(403,'can_include');
-- btree
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(783,'can_exclude');
-- gist
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(403,'no_such_property');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(999999,'can_order');
-- begin-expected
-- columns: pg_typeof:text
-- row: integer[]
-- rowcount: 1
-- end-expected
SELECT pg_typeof(pg_blocking_pids(1))::text;
