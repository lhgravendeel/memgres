-- source: review-2026-08.md
-- finding: Root cause 15: sequence names are looked up as raw text
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 15: sequence names are looked up as raw text
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_sa1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_sb1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_sa1.shared CACHE 5;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_sb1.shared CACHE 5 START 1000;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_sa1.shared');
-- begin-expected
-- columns: nextval:int8
-- row: 1000
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_sb1.shared');
-- begin-expected
-- columns: nextval:int8
-- row: 1001
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_sb1.shared');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_s;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('"zz_vf_s"');
-- begin-expected
-- columns: nextval:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT nextval('  zz_vf_s  ');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE "zz_vf_MiXeD";
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_mixed" does not exist
-- end-expected-error
SELECT nextval('zz_vf_MiXeD');
-- begin-expected
-- columns: nextval:int8
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT nextval(NULL);
-- begin-expected
-- columns: currval:int8
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT currval(NULL);
-- begin-expected
-- columns: setval:int8
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT setval(NULL, 1);
