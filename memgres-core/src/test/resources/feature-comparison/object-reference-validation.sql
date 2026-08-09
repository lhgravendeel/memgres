DROP TABLE IF EXISTS zz_ov_t CASCADE;

DROP TYPE IF EXISTS zz_ov_ct CASCADE;

DROP SEQUENCE IF EXISTS zz_ov_s CASCADE;

DROP FUNCTION IF EXISTS zz_ov_f(int);

DROP PROCEDURE IF EXISTS zz_ov_p();

DROP PROCEDURE IF EXISTS zz_ov_np(int,int);

CREATE TABLE zz_ov_t (a int);

CREATE TYPE zz_ov_ct AS (a int, b text);

CREATE SEQUENCE zz_ov_s;

CREATE FUNCTION zz_ov_f(a int) RETURNS int LANGUAGE sql AS $$ SELECT a $$;

CREATE PROCEDURE zz_ov_p() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;

CREATE PROCEDURE zz_ov_np(a int, b int, OUT c text) LANGUAGE plpgsql AS $$ BEGIN c := a::text || b::text; END $$;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: ERROR: relation "zz_ov_nosuch" does not exist
-- end-expected-error
COMMENT ON SEQUENCE zz_ov_nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: ERROR: relation "zz_ov_nosuch" does not exist
-- end-expected-error
COMMENT ON MATERIALIZED VIEW zz_ov_nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: role "zz_ov_nosuch" does not exist
-- end-expected-error
COMMENT ON ROLE zz_ov_nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: extension "zz_ov_nosuch" does not exist
-- end-expected-error
COMMENT ON EXTENSION zz_ov_nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: language "zz_ov_nosuch" does not exist
-- end-expected-error
COMMENT ON LANGUAGE zz_ov_nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: collation "zz_ov_nosuch" for encoding "UTF8" does not exist
-- end-expected-error
COMMENT ON COLLATION zz_ov_nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: event trigger "zz_ov_nosuch" does not exist
-- end-expected-error
COMMENT ON EVENT TRIGGER zz_ov_nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: aggregate zz_ov_nosuch(integer) does not exist
-- end-expected-error
COMMENT ON AGGREGATE zz_ov_nosuch(int) IS 'x';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: large object 987654321 does not exist
-- end-expected-error
COMMENT ON LARGE OBJECT 987654321 IS 'x';

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: "zz_ov_s" is not a table
-- end-expected-error
COMMENT ON TABLE zz_ov_s IS 'x';

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: function zz_ov_f(integer) is not an aggregate
-- end-expected-error
COMMENT ON AGGREGATE zz_ov_f(int) IS 'x';

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: zz_ov_p() is not a function
-- end-expected-error
COMMENT ON FUNCTION zz_ov_p() IS 'x';

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: zz_ov_f(integer) is not a procedure
-- end-expected-error
COMMENT ON PROCEDURE zz_ov_f(int) IS 'x';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: function zz_ov_f(text) does not exist
-- end-expected-error
COMMENT ON FUNCTION zz_ov_f(text) IS 'x';

COMMENT ON COLUMN zz_ov_ct.a IS 'ctcol';

-- begin-expected
-- columns: col_description
-- row: ctcol
-- end-expected
SELECT col_description('zz_ov_ct'::regclass, 1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuch" of relation "zz_ov_ct" does not exist
-- end-expected-error
COMMENT ON COLUMN zz_ov_ct.nosuch IS 'x';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: function pg_try_advisory_lock(bigint, integer) does not exist
-- end-expected-error
SELECT pg_try_advisory_lock(4294967296, 5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: function pg_try_advisory_lock(numeric) does not exist
-- end-expected-error
SELECT pg_try_advisory_lock(9223372036854775808);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: unsupported transaction command in PL/pgSQL
-- end-expected-error
DO $$ BEGIN START TRANSACTION; END $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: unsupported transaction command in PL/pgSQL
-- end-expected-error
DO $$ BEGIN SAVEPOINT sp; END $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: unsupported transaction command in PL/pgSQL
-- end-expected-error
DO $$ BEGIN ABORT; END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "TO"
-- end-expected-error
DO $$ BEGIN ROLLBACK TO SAVEPOINT sp; END $$;

-- begin-expected
-- columns: ?column?
-- row: after
-- end-expected
SELECT 'after';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: zero-length delimited identifier at or near """"
-- end-expected-error
LISTEN "";

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: zero-length delimited identifier at or near """"
-- end-expected-error
NOTIFY "";

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: zero-length delimited identifier at or near """"
-- end-expected-error
SELECT "";

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "'nokeyword'"
-- end-expected-error
NOTIFY zz_ov_ex 'nokeyword';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: unrecognized VACUUM option "bogus_option"
-- end-expected-error
VACUUM (BOGUS_OPTION);

-- begin-expected-error
-- sqlstate: 42P18
-- message-like: ERROR: cannot determine type of empty array
-- end-expected-error
SELECT ARRAY[];

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "void"
-- end-expected-error
CREATE PROCEDURE zz_ov_cb() RETURNS void LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "SETOF"
-- end-expected-error
CREATE PROCEDURE zz_ov_cc() RETURNS SETOF int LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: zz_ov_p() is a procedure
-- end-expected-error
SELECT * FROM zz_ov_p();

-- begin-expected
-- columns: c
-- row: 12
-- end-expected
CALL zz_ov_np(a => 1, b => 2, c => NULL);

-- begin-expected
-- columns: c
-- row: 12
-- end-expected
CALL zz_ov_np(a := 1, b := 2, c := NULL);

-- begin-expected
-- columns: c
-- row: 12
-- end-expected
CALL zz_ov_np(1, b => 2, c => NULL);

SET default_statistics_target = 100.7;

-- begin-expected
-- columns: current_setting
-- row: 101
-- end-expected
SELECT current_setting('default_statistics_target');

RESET default_statistics_target;

SET lock_timeout = '2500us';

-- begin-expected
-- columns: lock_timeout
-- row: 2ms
-- end-expected
SHOW lock_timeout;

RESET lock_timeout;

SET datestyle = 'ISO, YMD';

SET datestyle = 'ISO';

-- begin-expected
-- columns: current_setting
-- row: ISO, YMD
-- end-expected
SELECT current_setting('datestyle');

RESET datestyle;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: invalid value for parameter "TimeZone": "bogus/zone"
-- end-expected-error
SELECT set_config('TimeZone', 'bogus/zone', false);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: invalid value for parameter "client_encoding": "BOGUS"
-- end-expected-error
SELECT set_config('client_encoding', 'BOGUS', false);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: role "zz_ov_nosuchrole" does not exist
-- end-expected-error
SELECT set_config('role', 'zz_ov_nosuchrole', false);

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: ERROR: parameter "block_size" cannot be changed
-- end-expected-error
SET block_size TO DEFAULT;

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: ERROR: parameter "block_size" cannot be changed
-- end-expected-error
RESET block_size;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: payload string too long
-- end-expected-error
DO $$ BEGIN EXECUTE 'NOTIFY zz_ov_b, ' || quote_literal(repeat(U&'\00E9', 4000)); END $$;

DROP PROCEDURE zz_ov_np(int,int);

DROP PROCEDURE zz_ov_p();

DROP FUNCTION zz_ov_f(int);

DROP SEQUENCE zz_ov_s;

DROP TYPE zz_ov_ct;

DROP TABLE zz_ov_t;

