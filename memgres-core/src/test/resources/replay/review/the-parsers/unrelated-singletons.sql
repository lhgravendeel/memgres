-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: The parsers
-- title: Unrelated singletons
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "END"
-- end-expected-error
SELECT CASE 1 END;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ELSE"
-- end-expected-error
SELECT CASE 1 ELSE 2 END;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ix (arr int[]);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer[]
-- end-expected-error
CREATE INDEX zz_vf2_i3 ON zz_vf2_ix (arr COLLATE "C");
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf2_dom AS int;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_o3 (a zz_vf2_dom);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type public.zz_vf2_dom
-- end-expected-error
CREATE INDEX zz_vf2_dx ON zz_vf2_o3 (a COLLATE "C");
-- begin-expected-error
-- sqlstate: P0001
-- message-like: x
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'x' USING ERRCODE = '00000'; END $$;
