-- source: review-2026-08.md
-- finding: Root cause 5: DROP OWNED BY and ALTER DEFAULT PRIVILEGES touch ownership only
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 5: DROP OWNED BY and ALTER DEFAULT PRIVILEGES touch ownership only
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_base (i int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_base OWNER TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dep (i int REFERENCES zz_base(i));
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table zz_base because other objects depend on it
-- end-expected-error
DROP OWNED BY zz_a RESTRICT;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_class WHERE relname='zz_base';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
GRANT SELECT, INSERT ON zz_t TO zz_a;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_s" does not exist
-- end-expected-error
GRANT USAGE ON SCHEMA zz_s TO zz_a;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_s" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES FOR ROLE zz_a IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table zz_base because other objects depend on it
-- end-expected-error
DROP OWNED BY zz_a;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT has_table_privilege('zz_a','zz_t','SELECT')::text;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_s" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT, INSERT ON TABLES TO zz_a;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_s" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s REVOKE INSERT ON TABLES FROM zz_a;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_s" does not exist
-- end-expected-error
CREATE TABLE zz_s.t (i int);
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_s" does not exist
-- end-expected-error
SELECT has_table_privilege('zz_a','zz_s.t','SELECT')::text;
