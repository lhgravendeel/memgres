DROP TABLE IF EXISTS zz_sr_gen CASCADE;

DROP TABLE IF EXISTS zz_sr_own CASCADE;

DROP FUNCTION IF EXISTS zz_sr_bump() CASCADE;

DROP FUNCTION IF EXISTS zz_sr_exit() CASCADE;

DROP SEQUENCE IF EXISTS zz_sr_seq CASCADE;

DROP PROCEDURE IF EXISTS zz_sr_out(int, int);

DROP PROCEDURE IF EXISTS zz_sr_var(int[]);

DROP AGGREGATE IF EXISTS zz_sr_cat(text);

DROP ROLE IF EXISTS zz_sr_role;

DROP ROLE IF EXISTS zz_sr_a;

DROP ROLE IF EXISTS zz_sr_b;

CREATE TABLE zz_sr_gen (a int, g int GENERATED ALWAYS AS (a * 2) STORED);

CREATE FUNCTION zz_sr_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.a := NEW.a + 10; RETURN NEW; END $$;

CREATE TRIGGER zz_sr_gen_t BEFORE INSERT ON zz_sr_gen FOR EACH ROW EXECUTE FUNCTION zz_sr_bump();

INSERT INTO zz_sr_gen (a) VALUES (1);

-- begin-expected
-- columns: a|g
-- row: 11|22
-- end-expected
SELECT a, g FROM zz_sr_gen;

CREATE SEQUENCE zz_sr_seq;

PREPARE zz_sr_p AS SELECT nextval('zz_sr_seq') LIMIT 1;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: ERROR: currval of sequence "zz_sr_seq" is not yet defined in this session
-- end-expected-error
SELECT currval('zz_sr_seq');

PREPARE zz_sr_q(int) AS SELECT $1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: wrong number of parameters for prepared statement "zz_sr_q"
-- end-expected-error
EXECUTE zz_sr_q;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ")"
-- end-expected-error
EXECUTE zz_sr_q();

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: wrong number of parameters for prepared statement "zz_sr_q"
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_sr_q;

-- begin-expected-error
-- sqlstate: 26000
-- message-like: ERROR: prepared statement "zz_sr_absent" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_sr_absent;

PREPARE "ZzSrCase" AS SELECT 42;

-- begin-expected-error
-- sqlstate: 26000
-- message-like: ERROR: prepared statement "zzsrcase" does not exist
-- end-expected-error
EXECUTE zzsrcase;

BEGIN;

DECLARE "ZzSrCur" CURSOR FOR SELECT 7;

-- begin-expected
-- columns: ?column?
-- row: 7
-- end-expected
FETCH "ZzSrCur";

-- begin-expected-error
-- sqlstate: 34000
-- message-like: ERROR: cursor "zzsrcur" does not exist
-- end-expected-error
FETCH ALL FROM zzsrcur;

ROLLBACK;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: FOR UPDATE cannot be applied to VALUES
-- end-expected-error
DECLARE zz_sr_vals CURSOR FOR VALUES (1) FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: FOR UPDATE cannot be applied to VALUES
-- end-expected-error
VALUES (1) FOR UPDATE;

CREATE PROCEDURE zz_sr_out(a int, OUT b int) LANGUAGE plpgsql AS $$ BEGIN b := a * 2; END $$;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: procedure zz_sr_out(integer) does not exist
-- end-expected-error
CALL zz_sr_out(3);

-- begin-expected
-- columns: b
-- row: 6
-- end-expected
CALL zz_sr_out(3, NULL);

CREATE FUNCTION zz_sr_exit() RETURNS int LANGUAGE plpgsql AS $$ DECLARE n int := 0; BEGIN FOR i IN 1..3 LOOP BEGIN n := n + 1; EXIT WHEN n >= 2; EXCEPTION WHEN OTHERS THEN n := n + 100; END; END LOOP; RETURN n; END $$;

-- begin-expected
-- columns: zz_sr_exit
-- row: 2
-- end-expected
SELECT zz_sr_exit();

CREATE TABLE zz_sr_own (id int);

CREATE ROLE zz_sr_role NOLOGIN;

SET ROLE zz_sr_role;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: ERROR: must be owner of table zz_sr_own
-- end-expected-error
ALTER TABLE zz_sr_own ADD COLUMN z int;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: ERROR: must be owner of table zz_sr_own
-- end-expected-error
DROP TABLE zz_sr_own;

RESET ROLE;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM information_schema.tables WHERE table_name = 'zz_sr_own';

BEGIN;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchthing" does not exist
-- end-expected-error
SELECT nosuchthing;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "select"
-- end-expected-error
SAVEPOINT select;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "ALL"
-- end-expected-error
RELEASE SAVEPOINT ALL;

ROLLBACK;

CREATE ROLE zz_sr_a;

CREATE ROLE zz_sr_b;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: role "zz_sr_nosuch" does not exist
-- end-expected-error
GRANT zz_sr_a TO zz_sr_b GRANTED BY zz_sr_nosuch;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: ERROR: permission denied to grant privileges as role "zz_sr_a"
-- end-expected-error
GRANT zz_sr_a TO zz_sr_b GRANTED BY zz_sr_a;

-- begin-expected
-- columns: pg_typeof
-- row: integer
-- end-expected
SELECT pg_typeof(pg_backend_pid())::text;

-- begin-expected-error
-- sqlstate: 25001
-- message-like: ERROR: SET TRANSACTION ISOLATION LEVEL must be called before any query
-- end-expected-error
DO $$ BEGIN SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; END $$;

CREATE AGGREGATE zz_sr_cat (text) (SFUNC = textcat, STYPE = text, INITCOND = '');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: function zz_sr_cat(text, text) does not exist
-- end-expected-error
SELECT zz_sr_cat(v, v) FROM (VALUES ('a')) t(v);

DROP TABLE IF EXISTS zz_sr_gen CASCADE;

DROP TABLE IF EXISTS zz_sr_own CASCADE;

DROP FUNCTION IF EXISTS zz_sr_bump() CASCADE;

DROP FUNCTION IF EXISTS zz_sr_exit() CASCADE;

DROP SEQUENCE IF EXISTS zz_sr_seq CASCADE;

DROP PROCEDURE IF EXISTS zz_sr_out(int, int);

DROP AGGREGATE IF EXISTS zz_sr_cat(text);

DROP ROLE IF EXISTS zz_sr_role;

DROP ROLE IF EXISTS zz_sr_b;

DROP ROLE IF EXISTS zz_sr_a;

