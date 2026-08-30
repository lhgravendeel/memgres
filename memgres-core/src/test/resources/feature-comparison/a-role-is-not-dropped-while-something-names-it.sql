CREATE TABLE ii_t (i int);
CREATE ROLE ii_r1 NOLOGIN;
GRANT SELECT ON ii_t TO ii_r1;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: role "ii_r1" cannot be dropped because some objects depend on it
-- end-expected-error
DROP ROLE ii_r1;
REVOKE SELECT ON ii_t FROM ii_r1;
DROP ROLE ii_r1;
CREATE ROLE ii_r2 NOLOGIN;
CREATE SCHEMA ii_s AUTHORIZATION ii_r2;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: role "ii_r2" cannot be dropped because some objects depend on it
-- end-expected-error
DROP ROLE ii_r2;
DROP SCHEMA ii_s;
DROP ROLE ii_r2;
CREATE ROLE ii_r3 NOLOGIN;
GRANT USAGE ON SCHEMA public TO ii_r3;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: role "ii_r3" cannot be dropped because some objects depend on it
-- end-expected-error
DROP ROLE ii_r3;
DROP TABLE ii_t;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: role "ii_r3" cannot be dropped because some objects depend on it
-- end-expected-error
DROP ROLE IF EXISTS ii_r3;
