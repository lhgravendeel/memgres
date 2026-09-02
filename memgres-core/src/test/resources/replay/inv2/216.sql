-- source: investigation-2026-08.md
-- finding: 216
-- title: The CALL grammar is a plain expression list with no argument modifiers: parseCall loops on parser.parseExpression() with no named-argument (=> / :=) branch and 
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_np(a int, b int) LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
-- begin-expected
-- ok: -1
-- end-expected
CALL zz_vf2_np(a => 1, b => 2);
-- begin-expected
-- ok: -1
-- end-expected
CALL zz_vf2_np(1, b => 2);
-- begin-expected
-- ok: -1
-- end-expected
CALL zz_vf2_np(a := 1, b := 2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_vp(VARIADIC a int[]) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_vf2_log VALUES (a::text); END $$;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_log" does not exist
-- end-expected-error
CALL zz_vf2_vp(1,2,3);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_log" does not exist
-- end-expected-error
CALL zz_vf2_vp(VARIADIC ARRAY[4,5]);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_log" does not exist
-- end-expected-error
CALL zz_vf2_vp(VARIADIC '{8,9}'::int[]);
