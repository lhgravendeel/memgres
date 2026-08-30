-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET CONSTRAINTS ALL;
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "zj_no_such_constraint" does not exist
-- end-expected-error
SET CONSTRAINTS zj_no_such_constraint DEFERRED;
SET CONSTRAINTS ALL IMMEDIATE;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "END"
-- end-expected-error
SELECT CASE 1 END;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ELSE"
-- end-expected-error
SELECT CASE 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "END"
-- end-expected-error
SELECT CASE END;
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT CASE WHEN true THEN 1 END AS c;
-- begin-expected
-- columns: c
-- row: a
-- end-expected
SELECT CASE 1 WHEN 1 THEN 'a' ELSE 'b' END AS c;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "BOGUS"
-- end-expected-error
SELECT sum(g) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE BOGUS) FROM generate_series(1,3) g;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
SELECT sum(g) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING EXCLUDE) FROM generate_series(1,3) g;
-- begin-expected
-- columns: s
-- row: NULL
-- row: 1
-- row: 3
-- end-expected
SELECT sum(g) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) AS s FROM generate_series(1,3) g;
-- begin-expected
-- columns: s
-- row: 1
-- row: 3
-- row: 6
-- end-expected
SELECT sum(g) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING EXCLUDE NO OTHERS) AS s FROM generate_series(1,3) g;
CREATE ROLE zj_ra NOLOGIN;
CREATE ROLE zj_rb NOLOGIN;
CREATE ROLE zj_rc NOLOGIN;
DROP ROLE zj_ra, zj_rb, zj_rc;
-- begin-expected
-- columns: r
-- end-expected
SELECT rolname::text AS r FROM pg_roles WHERE rolname IN ('zj_ra','zj_rb','zj_rc') ORDER BY 1;
CREATE TABLE zj_o2 (a int, s text) WITH (toast.autovacuum_enabled = true);
CREATE TABLE zj_w4 (a int) WITH (autovacuum_enabled);
-- begin-expected
-- columns: o
-- row: {autovacuum_enabled=true}
-- end-expected
SELECT reloptions::text AS o FROM pg_class WHERE relname='zj_w4';
-- begin-expected
-- columns: o
-- row: NULL
-- end-expected
SELECT reloptions::text AS o FROM pg_class WHERE relname='zj_o2';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized parameter "nosuchoption"
-- end-expected-error
CREATE TABLE zj_o1 (a int) WITH (toast.nosuchoption = 1);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized parameter "fillfactor"
-- end-expected-error
CREATE TABLE zj_o3 (a int, s text) WITH (toast.fillfactor = 50);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for integer option "fillfactor": true
-- end-expected-error
CREATE TABLE zj_f (a int) WITH (fillfactor);
CREATE TABLE zj_e (a int, s text) WITH (toast.autovacuum_vacuum_threshold = 100, fillfactor = 70);
-- begin-expected
-- columns: o
-- row: {fillfactor=70}
-- end-expected
SELECT reloptions::text AS o FROM pg_class WHERE relname='zj_e';
CREATE TABLE zj_ix (arr int[], sarr text[]);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer[]
-- end-expected-error
CREATE INDEX zj_i1 ON zj_ix (arr COLLATE "C");
CREATE INDEX zj_i2 ON zj_ix (sarr COLLATE "C");
CREATE DOMAIN zj_di AS int;
CREATE DOMAIN zj_dt AS text;
CREATE TABLE zj_d (a zj_di, b zj_dt);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type public.zj_di
-- end-expected-error
CREATE INDEX zj_i3 ON zj_d (a COLLATE "C");
CREATE INDEX zj_i4 ON zj_d (b COLLATE "C");
CREATE TYPE zj_en AS ENUM ('a','b');
CREATE TYPE zj_co AS (x int);
CREATE TABLE zj_c (e zj_en, k zj_co);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type public.zj_en
-- end-expected-error
CREATE INDEX zj_i5 ON zj_c (e COLLATE "C");
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type public.zj_co
-- end-expected-error
CREATE INDEX zj_i6 ON zj_c (k COLLATE "C");
CREATE TYPE zj_ct AS (b boolean, s text, n int, d date);
-- begin-expected
-- columns: b | n | d
-- row: t | 1 | 2020-01-01
-- end-expected
SELECT ('(t,hi,1,2020-01-01)'::zj_ct).b AS b, ('(t,hi,1,2020-01-01)'::zj_ct).n AS n, ('(t,hi,1,2020-01-01)'::zj_ct).d AS d;
-- begin-expected
-- columns: b
-- row: t
-- end-expected
SELECT ((ROW(true,'hi',1,'2020-01-01')::zj_ct)::text::zj_ct).b AS b;
-- begin-expected
-- columns: s
-- row: a"b
-- end-expected
SELECT ('(t,"a""b",1,2020-01-01)'::zj_ct).s AS s;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "TimeZone": "168"
-- end-expected-error
SET TIME ZONE 168;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "TimeZone": "-168"
-- end-expected-error
SET TIME ZONE -168;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "TimeZone": "123456"
-- end-expected-error
SET TIME ZONE 123456;
SET TIME ZONE 167;
-- begin-expected
-- columns: TimeZone
-- row: <+167>-167
-- end-expected
SHOW TimeZone;
SET TIME ZONE 7;
-- begin-expected
-- columns: TimeZone
-- row: <+07>-07
-- end-expected
SHOW TimeZone;
SET TIME ZONE 'UTC';
CREATE ROLE zj_role NOLOGIN;
SET SESSION ROLE zj_role;
-- begin-expected
-- columns: current_role
-- row: zj_role
-- end-expected
SELECT current_role;
SET SESSION ROLE NONE;
SET ROLE zj_role;
-- begin-expected
-- columns: current_role
-- row: zj_role
-- end-expected
SELECT current_role;
RESET ROLE;
BEGIN;
SET LOCAL ROLE zj_role;
-- begin-expected
-- columns: current_role
-- row: zj_role
-- end-expected
SELECT current_role;
ROLLBACK;
DROP ROLE zj_role;
DROP TABLE zj_o2, zj_w4, zj_e, zj_ix, zj_d, zj_c CASCADE;
DROP DOMAIN zj_di, zj_dt;
DROP TYPE zj_en, zj_co, zj_ct;
