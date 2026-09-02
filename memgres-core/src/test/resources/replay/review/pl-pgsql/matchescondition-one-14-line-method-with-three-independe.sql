-- source: review-2026-08.md
-- finding: `matchesCondition` — one 14-line method with three independent bugs
-- area: PL/pgSQL
-- title: `matchesCondition` — one 14-line method with three independent bugs
-- begin-expected-error
-- sqlstate: 57014
-- message-like: 57014
-- end-expected-error
DO $$ begin raise sqlstate '57014'; exception when others then null; end $$;
-- begin-expected
-- ok: 0
-- end-expected
DO $$ begin perform 1/0; exception when sqlstate '22000' then null; end $$;
-- begin-expected
-- ok: 0
-- end-expected
DO $$ begin raise sqlstate '23514'; exception when sqlstate '23000' then null; end $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_others() RETURNS text AS $$
BEGIN
  BEGIN
    ASSERT false, 'boom';
  EXCEPTION WHEN OTHERS OR assert_failure THEN
    RETURN 'caught';
  END;
  RETURN 'no';
END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_others:text
-- row: caught
-- rowcount: 1
-- end-expected
SELECT zz_vf_others();
