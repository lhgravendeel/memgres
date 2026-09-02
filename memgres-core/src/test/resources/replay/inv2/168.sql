-- source: investigation-2026-08.md
-- finding: 168
-- title: CREATE COLLATION records a name and almost nothing else: the parser reads one identifier so a qualified name keeps only its first component, the executor matche
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_cs;
-- begin-expected
-- ok: 0
-- end-expected
CREATE COLLATION zz_cs.zz_sc (LOCALE = 'C');
-- begin-expected
-- columns: collname:name
-- row: zz_sc
-- rowcount: 1
-- end-expected
SELECT collname FROM pg_collation WHERE collname LIKE 'zz_%' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE COLLATION zz_c (LOCALE = 'C');
-- begin-expected
-- columns: s:text
-- row: A
-- row: B
-- row: a
-- row: b
-- rowcount: 4
-- end-expected
SELECT s FROM (VALUES ('B'),('a'),('A'),('b')) t(s) ORDER BY s COLLATE zz_c;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: collation "zz_c" for encoding "UTF8" already exists
-- end-expected-error
CREATE COLLATION zz_c (LOCALE = 'C');
-- begin-expected
-- columns: upper:text
-- row: STRAßE
-- rowcount: 1
-- end-expected
SELECT upper(U&'stra\00DFe' COLLATE zz_c);
-- begin-expected
-- ok: 0
-- end-expected
CREATE COLLATION zz_fc FROM "C";
-- begin-expected
-- columns: collcollate:text | collctype:text | collprovider:char
-- row: C | C | c
-- rowcount: 1
-- end-expected
SELECT collcollate, collctype, collprovider FROM pg_collation WHERE collname='zz_fc';
-- begin-expected-error
-- sqlstate: 42710
-- message-like: collation "zz_c" for encoding "UTF8" already exists
-- end-expected-error
CREATE COLLATION zz_c (LOCALE = 'C');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ct" does not exist
-- end-expected-error
CREATE INDEX zz_ci ON zz_ct (s COLLATE zz_c);
-- begin-expected
-- columns: pg_get_indexdef:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_get_indexdef((SELECT oid FROM pg_class WHERE relname='zz_ci'));
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: parameter "lc_ctype" must be specified
-- end-expected-error
CREATE COLLATION zz_c1 (LC_COLLATE = 'C');
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: parameter "lc_collate" must be specified
-- end-expected-error
CREATE COLLATION zz_c2 (PROVIDER = libc);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: collation attribute "nosuchopt" not recognized
-- end-expected-error
CREATE COLLATION zz_c3 (LOCALE = 'C', NOSUCHOPT = 'x');
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: unrecognized collation provider: nosuchprovider_zz
-- end-expected-error
CREATE COLLATION zz_c4 (LOCALE = 'C', PROVIDER = nosuchprovider_zz);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: could not create locale "zz_ZZ.nosuchlocale": No such file or directory
-- end-expected-error
CREATE COLLATION zz_c5 (LOCALE = 'zz_ZZ.nosuchlocale');
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: nondeterministic collations not supported with this provider
-- end-expected-error
CREATE COLLATION zz_c6 (LOCALE = 'C', DETERMINISTIC = FALSE);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "zz_nosuch_coll" for encoding "UTF8" does not exist
-- end-expected-error
CREATE COLLATION zz_c7 FROM zz_nosuch_coll;
