-- source: review-2026-08.md
-- finding: Root cause 7: CALL resolves a procedure by bare lowercased name and counts arguments; there is no signature
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 7: CALL resolves a procedure by bare lowercased name and counts arguments; there is no signature
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_dp(a int DEFAULT 1, b int DEFAULT 2, INOUT c int DEFAULT 3)
  LANGUAGE plpgsql AS $$ BEGIN c := a*100 + b*10 + c; END $$;
-- begin-expected
-- columns: c:int4
-- row: 123
-- rowcount: 1
-- end-expected
CALL zz_vf2_dp();
-- begin-expected
-- columns: c:int4
-- row: 923
-- rowcount: 1
-- end-expected
CALL zz_vf2_dp(9);
-- begin-expected
-- columns: c:int4
-- row: 983
-- rowcount: 1
-- end-expected
CALL zz_vf2_dp(9, 8);
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_op(a int, OUT b int) LANGUAGE plpgsql AS $$ BEGIN b := a*2; END $$;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zz_vf2_op(integer) does not exist
-- end-expected-error
CALL zz_vf2_op(3);
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_oq(OUT a int, OUT b text) LANGUAGE plpgsql AS $$ BEGIN a := 1; b := 'z'; END $$;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zz_vf2_oq() does not exist
-- end-expected-error
CALL zz_vf2_oq();
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_rp(a int) LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zz_vf2_rp(numeric) does not exist
-- end-expected-error
CALL zz_vf2_rp(1.5);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zz_vf2_rp(bigint) does not exist
-- end-expected-error
CALL zz_vf2_rp(2147483648);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zz_vf2_rp(boolean) does not exist
-- end-expected-error
CALL zz_vf2_rp(true);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "1.5"
-- end-expected-error
CALL zz_vf2_rp('1.5');
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in CALL argument
-- end-expected-error
CALL zz_vf2_rp((SELECT 3));
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in CALL arguments
-- end-expected-error
CALL zz_vf2_rp(count(*));
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf2_sch;
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_sch.zz_vf2_sp() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = public;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zz_vf2_sp() does not exist
-- end-expected-error
CALL zz_vf2_sp();
-- begin-expected-error
-- sqlstate: 42809
-- message-like: zz_vf2_rp(integer) is a procedure
-- end-expected-error
SELECT * FROM zz_vf2_rp(1);
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: procedure OUT parameters cannot appear after one with a default value
-- end-expected-error
CREATE PROCEDURE zz_vf2_ca(a int, b int DEFAULT 7, OUT c int) LANGUAGE plpgsql AS $$ BEGIN c := a+b; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "void"
-- end-expected-error
CREATE PROCEDURE zz_vf2_cb() RETURNS void LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SETOF"
-- end-expected-error
CREATE PROCEDURE zz_vf2_cc() RETURNS SETOF int LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: count(integer) is not a procedure
-- end-expected-error
CALL count(1);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: now() is not a procedure
-- end-expected-error
CALL now();
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_vf2_nosuchschema" does not exist
-- end-expected-error
CALL zz_vf2_nosuchschema.zz_vf2_rp(1);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: procedure zz_vf2_nosuch(integer) does not exist
-- end-expected-error
DROP PROCEDURE zz_vf2_nosuch(int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXISTS"
-- end-expected-error
DEALLOCATE IF EXISTS zz_vf2_nosuch;
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf2_dom AS int CHECK (VALUE > 0);
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_pdom (zz_vf2_dom) AS SELECT $1 AS v;
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zz_vf2_dom violates check constraint "zz_vf2_dom_check"
-- end-expected-error
EXECUTE zz_vf2_pdom(-1);
