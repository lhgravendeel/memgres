-- source: review-2026-08.md
-- finding: Root cause 15: definition-time validation is absent across DDL
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 15: definition-time validation is absent across DDL
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: parameter name "p" used more than once
-- end-expected-error
CREATE FUNCTION zz_cf1(p int, p int) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: parameter "parallel" must be SAFE, RESTRICTED, or UNSAFE
-- end-expected-error
CREATE FUNCTION zz_cf4() RETURNS int LANGUAGE sql PARALLEL nosuch AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: COST must be positive
-- end-expected-error
CREATE FUNCTION zz_cf5() RETURNS int LANGUAGE sql COST 0 AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: ROWS is not applicable when function does not return a set
-- end-expected-error
CREATE FUNCTION zz_cf6() RETURNS int LANGUAGE sql ROWS 100 AS $$ SELECT 1 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
REVOKE ALL PRIVILEGES ON zz_t FROM zz_nosuchrole;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: invalid privilege type SELECT for schema
-- end-expected-error
GRANT SELECT ON SCHEMA public TO zz_r;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: invalid privilege type INSERT for sequence
-- end-expected-error
GRANT INSERT ON SEQUENCE zz_s TO PUBLIC;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: role "zz_r" is a member of role "zz_r"
-- end-expected-error
GRANT zz_r TO zz_r;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "NOSUCHKIND"
-- end-expected-error
ALTER DEFAULT PRIVILEGES GRANT SELECT ON NOSUCHKIND TO zz_r;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t9 (i int, a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f9() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: trigger functions cannot have declared arguments
-- end-expected-error
CREATE FUNCTION zz_f9i(int) RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_f9i() does not exist
-- end-expected-error
CREATE TRIGGER zz_a AFTER INSERT ON zz_t9 FOR EACH ROW EXECUTE FUNCTION zz_f9i(1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: duplicate trigger events specified at or near "INSERT"
-- end-expected-error
CREATE TRIGGER zz_b AFTER INSERT OR INSERT ON zz_t9 FOR EACH ROW EXECUTE FUNCTION zz_f9();
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" specified more than once
-- end-expected-error
CREATE TRIGGER zz_c AFTER UPDATE OF a, a ON zz_t9 FOR EACH ROW EXECUTE FUNCTION zz_f9();
