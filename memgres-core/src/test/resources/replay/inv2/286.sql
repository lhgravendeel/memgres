-- source: investigation-2026-08.md
-- finding: 286
-- title: A family of catalog functions evaluates its argument for its own errors and then returns a constant, so the answer is the same whether the object exists or not 
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
