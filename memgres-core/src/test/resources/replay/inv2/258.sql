-- source: investigation-2026-08.md
-- finding: 258
-- title: The integer FOR loop is a Java `int` loop with no range or overflow check. `toInt` narrows the bounds with Number.intValue() and reports a generic message for N
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_wrap() RETURNS int AS $$
DECLARE n int := 0;
BEGIN
  FOR i IN 2147483646..2147483647 LOOP
    n := n + 1;
    EXIT WHEN n >= 5;   -- without this guard the loop runs ~4e9 more times
  END LOOP;
  RETURN n;
END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_wrap:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT zz_vf_wrap();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_big() RETURNS int AS $$
DECLARE n int := 0;
BEGIN
  FOR i IN 1..2147483648 LOOP n := n + 1; EXIT WHEN n >= 3; END LOOP;
  RETURN n;
END $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT zz_vf_big();
-- begin-expected-error
-- sqlstate: 22004
-- message-like: lower bound of FOR loop cannot be null
-- end-expected-error
DO $$ begin for i in null..3 loop null; end loop; end $$;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: upper bound of FOR loop cannot be null
-- end-expected-error
DO $$ begin for i in 1..null loop null; end loop; end $$;
