DROP TABLE IF EXISTS zz_sq_d CASCADE;

CREATE TABLE zz_sq_d (id int primary key, nm text);

INSERT INTO zz_sq_d VALUES (1,'dup'),(2,'dup'),(3,'x');

DROP PROCEDURE IF EXISTS zz_sq_pr(int);

DROP PROCEDURE IF EXISTS zz_sq_dp(text,int,int);

CREATE PROCEDURE zz_sq_pr(a int) LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;

CREATE PROCEDURE zz_sq_dp(OUT c text, a int DEFAULT 1, b int DEFAULT 2) LANGUAGE plpgsql AS $$ BEGIN c := a::text || b::text; END $$;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: step size cannot equal zero
-- end-expected-error
SELECT generate_series(1, 10, 0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: step size cannot equal zero
-- end-expected-error
SELECT generate_series(1::numeric, 5::numeric, 0::numeric);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: step size cannot equal zero
-- end-expected-error
SELECT generate_series('2000-01-01'::timestamp, '2000-01-05'::timestamp, '0 days'::interval);

-- begin-expected
-- columns: count
-- row: 20000
-- end-expected
SELECT count(*) FROM (SELECT generate_series(1::numeric, 20000::numeric, 1::numeric) AS g) t;

-- begin-expected
-- columns: count
-- row: 18264
-- end-expected
SELECT count(*) FROM (SELECT generate_series('2000-01-01'::timestamp, '2050-01-01'::timestamp, '1 day'::interval) AS g) t;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: setseed parameter 2 is out of allowed range [-1,1]
-- end-expected-error
SELECT setseed(2);

-- begin-expected-error
-- sqlstate: 2201G
-- message-like: ERROR: lower bound cannot equal upper bound
-- end-expected-error
SELECT width_bucket(1,2,2,1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "DEFAULT"
-- end-expected-error
SET ROLE DEFAULT;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: role "zz_sq_nosuchrole" does not exist
-- end-expected-error
SET ROLE zz_sq_nosuchrole;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: role "zz_sq_nosuchrole" does not exist
-- end-expected-error
SET SESSION AUTHORIZATION zz_sq_nosuchrole;

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: ERROR: parameter "block_size" cannot be changed
-- end-expected-error
RESET block_size;

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: ERROR: parameter "max_connections" cannot be changed without restarting the server
-- end-expected-error
RESET max_connections;

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: ERROR: parameter "wal_level" cannot be changed without restarting the server
-- end-expected-error
RESET wal_level;

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: ERROR: parameter "max_prepared_transactions" cannot be changed without restarting the server
-- end-expected-error
SET max_prepared_transactions = 10;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: ERROR: relation "zz_sq_absent" does not exist
-- end-expected-error
PREPARE zz_sq_p1 (int) AS SELECT s FROM zz_sq_absent WHERE id = $1;

-- begin-expected-error
-- sqlstate: 26000
-- message-like: ERROR: prepared statement "zz_sq_p1" does not exist
-- end-expected-error
EXECUTE zz_sq_p1(1);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ")"
-- end-expected-error
PREPARE zz_sq_p2 () AS SELECT 1 AS v;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: type "zz_sq_notatype" does not exist
-- end-expected-error
PREPARE zz_sq_p3 (zz_sq_notatype) AS SELECT $1 AS v;

-- begin-expected-error
-- sqlstate: 42P18
-- message-like: ERROR: could not determine data type of parameter $1
-- end-expected-error
PREPARE zz_sq_p4 AS SELECT $2 AS v;

-- begin-expected-error
-- sqlstate: 42P18
-- message-like: ERROR: could not determine data type of parameter $2
-- end-expected-error
PREPARE zz_sq_p5 AS SELECT $1 AS a, $3 AS c;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "||"
-- end-expected-error
COMMENT ON TABLE zz_sq_d IS 'a' || 'b';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "42"
-- end-expected-error
COMMENT ON TABLE zz_sq_d IS 42;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "current_user"
-- end-expected-error
COMMENT ON TABLE zz_sq_d IS current_user;

CALL zz_sq_pr(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: procedure zz_sq_pr(numeric) does not exist
-- end-expected-error
CALL zz_sq_pr(1.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: procedure zz_sq_pr(boolean) does not exist
-- end-expected-error
CALL zz_sq_pr(true);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: procedure zz_sq_pr(bigint) does not exist
-- end-expected-error
CALL zz_sq_pr(2147483648);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "RETURNING"
-- end-expected-error
CALL zz_sq_pr(1) RETURNING 1;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot use subquery in CALL argument
-- end-expected-error
CALL zz_sq_pr((SELECT 3));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: ERROR: aggregate functions are not allowed in CALL arguments
-- end-expected-error
CALL zz_sq_pr(count(*));

-- begin-expected
-- columns: c
-- row: 12
-- end-expected
CALL zz_sq_dp(NULL);

-- begin-expected
-- columns: c
-- row: 92
-- end-expected
CALL zz_sq_dp(NULL, 9);

-- begin-expected
-- columns: c
-- row: 98
-- end-expected
CALL zz_sq_dp(NULL, 9, 8);

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: ERROR: procedure OUT parameters cannot appear after one with a default value
-- end-expected-error
CREATE PROCEDURE zz_sq_bad(a int DEFAULT 1, OUT c text) LANGUAGE plpgsql AS $$ BEGIN c := 'x'; END $$;

-- begin-expected-error
-- sqlstate: 34000
-- message-like: ERROR: cursor "zz_sq_nocursor" does not exist
-- end-expected-error
UPDATE zz_sq_d SET nm='z' WHERE CURRENT OF zz_sq_nocursor;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ERROR: LOCK TABLE can only be used in transaction blocks
-- end-expected-error
LOCK TABLE zz_sq_d IN ACCESS EXCLUSIVE MODE;

DO $$ BEGIN LOCK TABLE zz_sq_d IN ACCESS EXCLUSIVE MODE; END $$;

DROP PROCEDURE zz_sq_dp(text,int,int);

DROP PROCEDURE zz_sq_pr(int);

DROP TABLE zz_sq_d;

