-- Session GUC fidelity tests (H35, H36, H37, M13, L7, L8)

-- H35: Temp table should not shadow explicitly schema-qualified names
CREATE TABLE public.h35_t(id int, val text);
INSERT INTO public.h35_t VALUES (1, 'permanent');
CREATE TEMP TABLE h35_t(id int, val text);
INSERT INTO h35_t VALUES (2, 'temp');
SELECT val FROM public.h35_t; -- expect 'permanent', not 'temp'

-- H35: Empty search_path should fail to resolve unqualified names
SET search_path = '';
SELECT * FROM h35_t; -- should fail 42P01 (no temp implicit in empty path)
SET search_path = '"$user", public';

-- H36: SET SESSION AUTHORIZATION should update current_user/session_user
SET SESSION AUTHORIZATION DEFAULT;
SELECT current_user, session_user;

-- H36: SET ROLE to nonexistent role should error
SET ROLE nonexistent_role_xyz; -- expect 22023

-- H36: SET SESSION AUTHORIZATION to nonexistent user should error
SET SESSION AUTHORIZATION nonexistent_user_xyz; -- expect 22023

-- H37: Invalid SET values should be rejected
SET datestyle = 'bogus'; -- expect error 22023
SET row_security = 'maybe'; -- expect error 22023
SET statement_timeout = 'notanum'; -- expect error 22023
SET datestyle = 'ISO, DMY'; -- should succeed and store correctly
SHOW datestyle; -- expect 'ISO, DMY'

-- M13: Plain SET should be rolled back on ROLLBACK
SET application_name = 'original';
BEGIN;
SET application_name = 'changed';
ROLLBACK;
SHOW application_name; -- expect 'original'

-- M13: SET LOCAL outside transaction should warn and not persist
SET LOCAL application_name = 'local_outside';
SHOW application_name; -- expect 'original' (unchanged)

-- M13: set_config with is_local=true outside txn should not persist
SELECT set_config('application_name', 'via_set_config', true);
SHOW application_name; -- expect 'original' (unchanged)

-- L7: DISCARD ALL should reset application_name to empty string
SET application_name = 'test_app';
DISCARD ALL;
SHOW application_name; -- expect '' (empty string)

-- L7: SHOW search_path should include quotes around "$user"
SET search_path = '"$user", public';
SHOW search_path; -- expect '"$user", public'

-- L7: current_setting(missing, true) after RESET should return NULL
SET custom.test_var = 'hello';
RESET custom.test_var;
SELECT current_setting('custom.test_var', true); -- expect NULL

-- L8: CLUSTER without previously clustered index
CREATE TABLE l8_t(id int);
CLUSTER l8_t; -- expect error 42704
DROP TABLE l8_t;
