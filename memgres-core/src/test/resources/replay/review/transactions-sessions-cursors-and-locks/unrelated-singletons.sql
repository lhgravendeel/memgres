-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Transactions, sessions, cursors and locks
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '13MB';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET LOCAL work_mem TO DEFAULT;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- columns: work_mem:text
-- row: 13MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- begin-expected
-- columns: transaction_isolation:text
-- row: read committed
-- rowcount: 1
-- end-expected
SHOW transaction_isolation;
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf_xp AS SELECT $1::int;
-- begin-expected
-- columns: statement:text | parameter_types:text
-- row: PREPARE zz_vf_xp AS SELECT $1::int | {integer}
-- rowcount: 1
-- end-expected
SELECT statement, parameter_types::text FROM pg_prepared_statements WHERE name='zz_vf_xp';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1;
-- begin-expected
-- columns: n:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_current_xact_id_if_assigned() IS NULL AS n;
-- begin-expected
-- columns: current_setting:text
-- row: none
-- rowcount: 1
-- end-expected
SELECT current_setting('role');
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_perm_r NOLOGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_perm_r;
-- begin-expected
-- columns: current_setting:text
-- row: off
-- rowcount: 1
-- end-expected
SELECT current_setting('is_superuser');
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_r (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_rowcount() RETURNS int AS $$
DECLARE n int;
BEGIN
  BEGIN
    INSERT INTO zz_vf_r VALUES (1), (2), (3);
  END;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_rowcount:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT zz_vf_rowcount();
-- begin-expected
-- ok: 0
-- end-expected
CREATE EXTENSION IF NOT EXISTS hstore;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT slice('a=>1, b=>2'::hstore, NULL) IS NULL;
-- begin-expected
-- ok: 0
-- end-expected
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT similarity('abc', NULL) IS NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT word_similarity('abc', NULL) IS NULL;
-- begin-expected
-- columns: unicode_version:text
-- row: 16.0
-- rowcount: 1
-- end-expected
SELECT unicode_version();
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_vf_x4 ASENSITIVE CURSOR FOR SELECT 1;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
LOCK TABLE pg_class IN ACCESS SHARE MODE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_nt(id int, nm text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_nt VALUES (1,'a'),(2,'b');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_k18() RETURNS text AS $$
declare c cursor for select nm from zz_vf_nt order by id; r record; s text := '';
begin for r in c loop s := s || r.nm; end loop; return s; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_k18:text
-- row: ab
-- rowcount: 1
-- end-expected
SELECT zz_vf_k18();
-- begin-expected
-- columns: user:name
-- row: memgres
-- rowcount: 1
-- end-expected
SELECT user;
-- begin-expected
-- columns: session_authorization:text
-- row: memgres
-- rowcount: 1
-- end-expected
SHOW SESSION AUTHORIZATION;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_isolation_test_session_is_blocked(1, '{}') IS NOT NULL;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_rc() RETURNS refcursor LANGUAGE plpgsql AS $$ DECLARE r refcursor; BEGIN OPEN r FOR SELECT 1; RETURN r; END $$;
-- JDBC: prepareCall("{? = call zz_vf_rc()}"), registerOutParameter(1, Types.REF_CURSOR)
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ALL"
-- end-expected-error
SAVEPOINT ALL;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "select"
-- end-expected-error
SAVEPOINT select;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ALL"
-- end-expected-error
RELEASE SAVEPOINT ALL;
-- begin-expected
-- columns: xml_is_well_formed:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT xml_is_well_formed(repeat('<a>', 257) || repeat('</a>', 257));
-- begin-expected-error
-- sqlstate: 2200N
-- message-like: invalid XML content
-- end-expected-error
SELECT (repeat('<a>', 300) || repeat('</a>', 300))::xml IS NOT NULL;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: snapshot "00000003-0000001B-1" does not exist
-- end-expected-error
SET TRANSACTION SNAPSHOT '00000003-0000001B-1';
-- begin-expected
-- ok: 0
-- end-expected
PREPARE TRANSACTION 'zz_vf_gid';
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- outside a transaction
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- outside a transaction
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot use special role specifier in DROP ROLE
-- end-expected-error
DROP ROLE current_user;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "zz_vf_no_such_role" does not exist
-- end-expected-error
SET ROLE zz_vf_no_such_role;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "IF"
-- end-expected-error
CREATE ROLE zz_vf_r IF NOT EXISTS;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_perm" does not exist
-- end-expected-error
SELECT * FROM zz_vf_perm;
-- as a role without SELECT
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: return type mismatch in function declared to return integer
-- end-expected-error
CREATE FUNCTION zz_vf_rt() RETURNS int LANGUAGE sql AS $$ SELECT 'a' $$;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type point to integer
-- end-expected-error
SELECT point '(1,2)'::int;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type integer[] to integer
-- end-expected-error
SELECT '{1}'::int[]::int;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pk2 (id int PRIMARY KEY, c int);
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: multiple primary keys for table "zz_vf_pk2" are not allowed
-- end-expected-error
ALTER TABLE zz_vf_pk2 ADD CONSTRAINT zz_vf_pk2_pkey PRIMARY KEY (c);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET x;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized partitioning strategy "bogus"
-- end-expected-error
CREATE TABLE zz_vf_pb (a int) PARTITION BY BOGUS (a);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT 1 GROUP;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT * FROM t ORDER;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
CREATE TABLE t (a);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT (1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
SELECT 1..2;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FROM"
-- end-expected-error
SELECT EXTRACT(FROM now());
-- begin-expected-error
-- sqlstate: 42601
-- message-like: VALUES lists must all be the same length
-- end-expected-error
VALUES (1), (1,2);
