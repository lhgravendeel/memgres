-- source: investigation-2026-08.md
-- finding: 164
-- title: ALTER POLICY rebuilds the policy through the five-argument RlsPolicy constructor, which hardcodes "PERMISSIVE" and re-uses the policy's existing role list, so R
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_rr" does not exist
-- end-expected-error
CREATE POLICY zz_perm ON zz_rt FOR SELECT TO zz_rr USING (true);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_rr" does not exist
-- end-expected-error
CREATE POLICY zz_restr ON zz_rt AS RESTRICTIVE FOR SELECT TO zz_rr USING (id = 1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rt" does not exist
-- end-expected-error
ALTER POLICY zz_restr ON zz_rt USING (id <= 2);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "zz_rr" does not exist
-- end-expected-error
SET ROLE zz_rr;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rt" does not exist
-- end-expected-error
SELECT count(*) FROM zz_rt;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_pt" does not exist
-- end-expected-error
CREATE POLICY zz_p1 ON zz_pt FOR SELECT USING (id = 1);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_r1" does not exist
-- end-expected-error
ALTER POLICY zz_p1 ON zz_pt TO zz_r1;
-- begin-expected
-- columns: policyname:name | roles:text
-- rowcount: 0
-- end-expected
SELECT policyname, roles::text FROM pg_policies WHERE tablename='zz_pt';
