-- source: investigation-2026-08.md
-- finding: 264
-- title: PL/pgSQL's own control-flow signals are ordinary RuntimeExceptions, and the BEGIN … EXCEPTION handler catches RuntimeException wholesale. An EXIT or CONTINUE cr
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
