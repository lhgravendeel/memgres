-- rls-enforcement.sql
-- Verification file for C7: Row-Level Security enforcement

-- === Default-deny: RLS enabled with no policies → non-owner sees nothing ===

CREATE TABLE rls_deny(id int, val text);
INSERT INTO rls_deny VALUES (1, 'a'), (2, 'b'), (3, 'c');
ALTER TABLE rls_deny ENABLE ROW LEVEL SECURITY;

CREATE ROLE rls_user LOGIN;
GRANT ALL ON rls_deny TO rls_user;

SET ROLE rls_user;
SELECT count(*)::int FROM rls_deny; -- expect: 0 (default-deny, no policies)
DELETE FROM rls_deny; -- expect: 0 rows affected (default-deny)
UPDATE rls_deny SET val = 'x'; -- expect: 0 rows affected (default-deny)
INSERT INTO rls_deny VALUES (4, 'd'); -- expect: error 42501 (no INSERT policy)
RESET ROLE;

-- Owner should see all rows (no FORCE)
SELECT count(*)::int FROM rls_deny; -- expect: 3

DROP TABLE rls_deny;
DROP ROLE rls_user;

-- === DELETE policy enforcement ===

CREATE TABLE rls_del(id int, owner_name text);
INSERT INTO rls_del VALUES (1, 'alice'), (2, 'bob'), (3, 'alice');
ALTER TABLE rls_del ENABLE ROW LEVEL SECURITY;

CREATE ROLE alice LOGIN;
CREATE ROLE bob LOGIN;
GRANT ALL ON rls_del TO alice, bob;

-- Policy: users can only see/modify their own rows
CREATE POLICY rls_del_sel ON rls_del FOR SELECT USING (owner_name = current_user);
CREATE POLICY rls_del_del ON rls_del FOR DELETE USING (owner_name = current_user);

SET ROLE alice;
SELECT count(*)::int FROM rls_del; -- expect: 2 (alice's rows)
DELETE FROM rls_del WHERE id = 2; -- expect: 0 rows (bob's row, not visible)
DELETE FROM rls_del WHERE id = 1; -- expect: 1 row
RESET ROLE;

SET ROLE bob;
SELECT count(*)::int FROM rls_del; -- expect: 1 (bob's row)
DELETE FROM rls_del; -- expect: 1 row (bob can only delete own row)
RESET ROLE;

SELECT count(*)::int FROM rls_del; -- expect: 1 (alice's id=3 remains)

DROP TABLE rls_del;
DROP ROLE alice;
DROP ROLE bob;

-- === UPDATE WITH CHECK enforcement ===

CREATE TABLE rls_upd(id int, owner_name text);
INSERT INTO rls_upd VALUES (1, 'upduser'), (2, 'other');
ALTER TABLE rls_upd ENABLE ROW LEVEL SECURITY;

CREATE ROLE upduser LOGIN;
GRANT ALL ON rls_upd TO upduser;

-- upduser can see and update own rows; new row must still be owned
CREATE POLICY rls_upd_all ON rls_upd FOR ALL USING (owner_name = current_user) WITH CHECK (owner_name = current_user);

SET ROLE upduser;
SELECT count(*)::int FROM rls_upd; -- expect: 1 (only own row)
UPDATE rls_upd SET owner_name = 'stolen' WHERE id = 1; -- expect: error 42501 (WITH CHECK fails)
RESET ROLE;

DROP TABLE rls_upd;
DROP ROLE upduser;

-- === Owner bypass vs FORCE distinction ===

CREATE TABLE rls_force(id int, val text);
INSERT INTO rls_force VALUES (1, 'a'), (2, 'b');
ALTER TABLE rls_force ENABLE ROW LEVEL SECURITY;
CREATE POLICY rls_force_sel ON rls_force FOR SELECT USING (id = 1);

-- Owner sees all rows (not affected by RLS)
SELECT count(*)::int FROM rls_force; -- expect: 2

ALTER TABLE rls_force FORCE ROW LEVEL SECURITY;
-- Owner now affected by RLS policies
SELECT count(*)::int FROM rls_force; -- expect: 1 (only id=1 passes policy)

ALTER TABLE rls_force NO FORCE ROW LEVEL SECURITY;
SELECT count(*)::int FROM rls_force; -- expect: 2 (owner bypass restored)

DROP TABLE rls_force;

-- === Superuser bypass uses rolsuper attribute ===

CREATE ROLE rls_super SUPERUSER LOGIN;
CREATE TABLE rls_su(id int);
INSERT INTO rls_su VALUES (1), (2);
ALTER TABLE rls_su ENABLE ROW LEVEL SECURITY;
GRANT ALL ON rls_su TO rls_super;

SET ROLE rls_super;
SELECT count(*)::int FROM rls_su; -- expect: 2 (superuser bypasses RLS)
RESET ROLE;

DROP TABLE rls_su;
DROP ROLE rls_super;
