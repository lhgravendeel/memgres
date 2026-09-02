-- source: review-2026-08.md
-- finding: Failures during argument and typmod evaluation are caught and replaced by a default
-- area: PL/pgSQL
-- title: Failures during argument and typmod evaluation are caught and replaced by a default
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_dflt(a int, b int DEFAULT 1/0) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN b; END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zz_vf_dflt(a => 5);
-- named-argument path
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT zz_vf_dflt(5);
-- positional path
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "99999999999"
-- end-expected-error
CREATE FUNCTION zz_vf_vcbig() RETURNS int AS $$ DECLARE v varchar(99999999999) := 'abcdef'; BEGIN RETURN length(v); END $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf_vcbig() does not exist
-- end-expected-error
SELECT zz_vf_vcbig();
