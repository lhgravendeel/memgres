-- source: review-2026-08.md
-- finding: Root cause 10: ErrorResponse has no Where/internal-position/internal-query fields
-- area: Wire protocol and error-report fidelity
-- title: Root cause 10: ErrorResponse has no Where/internal-position/internal-query fields
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f6() RETURNS int LANGUAGE plpgsql AS $$ DECLARE r int;
  BEGIN EXECUTE 'SELECT nosuch_zz_vf' INTO r; RETURN r; END $$;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_zz_vf" does not exist
-- end-expected-error
SELECT zz_vf_f6();
