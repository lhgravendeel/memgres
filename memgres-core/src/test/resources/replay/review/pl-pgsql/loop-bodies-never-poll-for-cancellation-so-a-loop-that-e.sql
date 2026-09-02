-- source: review-2026-08.md
-- finding: Loop bodies never poll for cancellation, so a loop that evaluates nothing wedges the connection permanently
-- area: PL/pgSQL
-- title: Loop bodies never poll for cancellation, so a loop that evaluates nothing wedges the connection permanently
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_spin() RETURNS void LANGUAGE plpgsql AS $$ BEGIN LOOP NULL; END LOOP; END $$;
-- begin-expected
-- ok: 0
-- end-expected
SET statement_timeout = '2s';
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
SELECT zz_vf_spin();
-- begin-expected-error
-- sqlstate: 42601
-- message-like: missing expression at or near ";"
-- end-expected-error
DO $$ begin loop exit when; end loop; end $$;
