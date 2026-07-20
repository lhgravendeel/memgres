-- privilege-enforcement.sql
-- Verification file for C6, M9, M10, M11, M12

-- === C6: Privilege enforcement at execution time ===

CREATE ROLE nopriv LOGIN;
CREATE TABLE priv_test(id int, v text);
INSERT INTO priv_test VALUES (1, 'hello');

-- As nopriv, all operations should fail with 42501
SET ROLE nopriv;
SELECT * FROM priv_test; -- expect: error 42501
INSERT INTO priv_test VALUES (2, 'x'); -- expect: error 42501
UPDATE priv_test SET v = 'y' WHERE id = 1; -- expect: error 42501
DELETE FROM priv_test WHERE id = 1; -- expect: error 42501
TRUNCATE priv_test; -- expect: error 42501
RESET ROLE;

-- Grant SELECT only
GRANT SELECT ON priv_test TO nopriv;
SET ROLE nopriv;
SELECT * FROM priv_test; -- expect: success
INSERT INTO priv_test VALUES (3, 'z'); -- expect: error 42501
RESET ROLE;

-- Grant INSERT
GRANT INSERT ON priv_test TO nopriv;
SET ROLE nopriv;
INSERT INTO priv_test VALUES (3, 'z'); -- expect: success
RESET ROLE;

-- Revoke SELECT
REVOKE SELECT ON priv_test FROM nopriv;
SET ROLE nopriv;
SELECT * FROM priv_test; -- expect: error 42501
RESET ROLE;

DROP TABLE priv_test;
DROP ROLE nopriv;

-- === M9: Grant-option semantics ===

CREATE ROLE grantor_role LOGIN;
CREATE ROLE grantee_role LOGIN;
CREATE TABLE m9_test(id int);

GRANT SELECT ON m9_test TO grantor_role WITH GRANT OPTION;

-- grantor_role should be able to grant SELECT (which they hold)
SET ROLE grantor_role;
GRANT SELECT ON m9_test TO grantee_role; -- expect: success

-- grantor_role should NOT be able to grant INSERT (which they don't hold)
GRANT INSERT ON m9_test TO grantee_role; -- expect: error 42501
RESET ROLE;

-- has_table_privilege should distinguish WITH GRANT OPTION
SELECT has_table_privilege('grantor_role', 'm9_test', 'SELECT'); -- expect: true
SELECT has_table_privilege('grantor_role', 'm9_test', 'SELECT WITH GRANT OPTION'); -- expect: true
SELECT has_table_privilege('grantor_role', 'm9_test', 'INSERT WITH GRANT OPTION'); -- expect: false

DROP TABLE m9_test;
DROP ROLE grantee_role;
DROP ROLE grantor_role;

-- === M10: Schema-qualified GRANT/REVOKE ===

CREATE SCHEMA m10_schema;
CREATE TABLE m10_schema.m10_test(id int);
CREATE ROLE m10_role LOGIN;

GRANT SELECT ON m10_schema.m10_test TO m10_role; -- expect: success (not 42P01)
SET ROLE m10_role;
SELECT * FROM m10_schema.m10_test; -- expect: success
RESET ROLE;

DROP TABLE m10_schema.m10_test;
DROP SCHEMA m10_schema;
DROP ROLE m10_role;

-- === M11: ALTER DEFAULT PRIVILEGES applied to new objects ===

CREATE ROLE m11_creator LOGIN;
CREATE ROLE m11_reader LOGIN;

ALTER DEFAULT PRIVILEGES FOR ROLE m11_creator GRANT SELECT ON TABLES TO m11_reader;

SET ROLE m11_creator;
CREATE TABLE m11_auto(id int);
RESET ROLE;

-- m11_reader should have SELECT on m11_auto (auto-granted by default privileges)
SET ROLE m11_reader;
SELECT * FROM m11_auto; -- expect: success (not 42501)
RESET ROLE;

DROP TABLE m11_auto;
DROP ROLE m11_reader;
DROP ROLE m11_creator;

-- === M12: CREATE ROLE IN ROLE membership ===

CREATE ROLE m12_group;
CREATE ROLE m12_member IN ROLE m12_group;

SELECT pg_has_role('m12_member', 'm12_group', 'MEMBER'); -- expect: true

DROP ROLE m12_member;
DROP ROLE m12_group;
