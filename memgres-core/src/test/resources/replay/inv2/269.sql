-- source: investigation-2026-08.md
-- finding: 269
-- title: A role OID can never be turned back into a role name. pg_get_userbyid returns the string literal "memgres" for any argument, pg_tables.tableowner is that same l
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_o1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_or;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_o1 OWNER TO zz_or;
-- begin-expected
-- columns: tableowner:name
-- row: zz_or
-- rowcount: 1
-- end-expected
SELECT tableowner FROM pg_tables WHERE tablename='zz_o1';
-- begin-expected
-- columns: pg_get_userbyid:name
-- row: zz_or
-- rowcount: 1
-- end-expected
SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname='zz_o1';
-- begin-expected
-- columns: pg_get_userbyid:name
-- row: unknown (OID=999999)
-- rowcount: 1
-- end-expected
SELECT pg_get_userbyid(999999);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_o1" already exists
-- end-expected-error
CREATE TABLE zz_o1 (a int);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_or" already exists
-- end-expected-error
CREATE ROLE zz_or;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_o1 OWNER TO zz_or;
-- begin-expected
-- columns: relowner:text
-- row: zz_or
-- rowcount: 1
-- end-expected
SELECT relowner::regrole::text FROM pg_class WHERE relname='zz_o1';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_no_such_role" does not exist
-- end-expected-error
SELECT 'zz_no_such_role'::regrole::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ht (a int, b text);
-- begin-expected
-- columns: a:text | b:text | c:text
-- row: NULL | false | NULL
-- rowcount: 1
-- end-expected
SELECT has_table_privilege(999999999::oid, 'SELECT')::text AS a,
       has_table_privilege(999999999::oid, 'zz_ht', 'SELECT')::text AS b,
       has_table_privilege('memgres', 999999999::oid, 'SELECT')::text AS c;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_ht" already exists
-- end-expected-error
CREATE TABLE zz_ht (a int, b text);
-- begin-expected
-- columns: has_column_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_column_privilege('zz_ht'::regclass::oid, 1::smallint, 'SELECT')::text;
-- begin-expected
-- columns: has_schema_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_schema_privilege('public'::regnamespace::oid, 'USAGE')::text;
