-- source: investigation-2026-08.md
-- finding: 389
-- title: sendErrorWithDetails writes S,V,C,M and optionally D,H,P,s,t,c,n,d plus stub F/L/R. No code path anywhere in the pgwire package ever writes a W (Where/CONTEXT),
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
