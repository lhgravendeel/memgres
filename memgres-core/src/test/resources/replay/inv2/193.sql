-- source: investigation-2026-08.md
-- finding: 193
-- title: The PL/pgSQL block executor catches every RuntimeException to look for an EXCEPTION handler, but ExitSignal and ContinueSignal are RuntimeExceptions too — only 
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_exit() RETURNS int AS $$
DECLARE n int := 0;
BEGIN
  FOR i IN 1..3 LOOP
    BEGIN
      n := n + 1;
      EXIT WHEN n >= 2;
    EXCEPTION WHEN OTHERS THEN
      n := n + 100;
    END;
  END LOOP;
  RETURN n;
END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_exit:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT zz_vf_exit();
