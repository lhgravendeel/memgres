-- source: review-2026-08.md
-- finding: PL/pgSQL's control-flow signals are RuntimeExceptions, and the exception handler catches them
-- area: PL/pgSQL
-- title: PL/pgSQL's control-flow signals are RuntimeExceptions, and the exception handler catches them
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_exit1() RETURNS int AS $$
DECLARE n int := 0;
BEGIN
  FOR i IN 1..3 LOOP
    BEGIN
      n := n + 1;
      EXIT;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END LOOP;
  RETURN n;
END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_exit1:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_vf_exit1();
