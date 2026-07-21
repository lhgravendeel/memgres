-- ============================================================================
-- Feature Comparison: Session name-resolution & GUC residuals
-- Target: PostgreSQL 18 vs Memgres
-- Covers: H35 (temp shadowing in DML), H37 (DateStyle date input),
--         L7 (current_setting after RESET), L8 (CLUSTER remembers index).
-- H36 (RESET SESSION AUTHORIZATION) is covered by JDBC unit tests; the
-- current_user round-trip is included here in a connection-user-agnostic form.
-- ============================================================================

-- ============================================================================
-- H35: an explicit public.x qualifier in UPDATE/INSERT/DELETE must NOT be
--      shadowed by a same-named temp table.
-- ============================================================================

DROP TABLE IF EXISTS public.resid_h35 CASCADE;
CREATE TABLE public.resid_h35 (v text);
-- The shared PG test DB has a FOR ALL TABLES publication; a scratch table without
-- a PK needs REPLICA IDENTITY FULL for UPDATE/DELETE to be allowed.
ALTER TABLE public.resid_h35 REPLICA IDENTITY FULL;
INSERT INTO public.resid_h35 VALUES ('permanent');
CREATE TEMP TABLE resid_h35 (v text);
INSERT INTO resid_h35 VALUES ('temp');

-- UPDATE via explicit public.x hits the permanent table
UPDATE public.resid_h35 SET v = 'updated';

-- begin-expected
-- columns: v
-- row: updated
-- end-expected
SELECT v FROM public.resid_h35;

-- the temp table is untouched
-- begin-expected
-- columns: v
-- row: temp
-- end-expected
SELECT v FROM pg_temp.resid_h35;

-- INSERT via explicit public.x hits the permanent table
INSERT INTO public.resid_h35 VALUES ('added');

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::integer AS c FROM public.resid_h35;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::integer AS c FROM pg_temp.resid_h35;

-- DELETE via explicit public.x hits the permanent table
DELETE FROM public.resid_h35;

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::integer AS c FROM public.resid_h35;

-- temp still holds its row
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::integer AS c FROM pg_temp.resid_h35;

DROP TABLE pg_temp.resid_h35;
DROP TABLE public.resid_h35;

-- ============================================================================
-- H37: DateStyle field order (DMY / YMD / MDY) applied to date INPUT parsing.
--      Output style stays ISO so the rendered value is yyyy-mm-dd.
-- ============================================================================

SET datestyle = 'ISO, DMY';

-- begin-expected
-- columns: a | b
-- row: 2026-02-01, 2026-07-13
-- end-expected
SELECT '01/02/2026'::date AS a, '13/07/2026'::date AS b;

SET datestyle = 'ISO, MDY';

-- begin-expected
-- columns: a
-- row: 2026-01-02
-- end-expected
SELECT '01/02/2026'::date AS a;

SET datestyle = 'ISO, YMD';

-- begin-expected
-- columns: a
-- row: 2026-07-13
-- end-expected
SELECT '2026/07/13'::date AS a;

-- DMY also applies to date INPUT stored in a date column
SET datestyle = 'ISO, DMY';
DROP TABLE IF EXISTS resid_h37 CASCADE;
CREATE TABLE resid_h37 (d date);
INSERT INTO resid_h37 VALUES ('13/07/2026');

-- begin-expected
-- columns: d
-- row: 2026-07-13
-- end-expected
SELECT d FROM resid_h37;

DROP TABLE resid_h37;
SET datestyle = 'ISO, MDY';

-- ============================================================================
-- L7: current_setting('custom.x', true) after RESET returns '' (empty),
--     not NULL, because the placeholder stays defined for the session.
-- ============================================================================

SET "custom.resid_l7" = 'hello';
RESET "custom.resid_l7";

-- After RESET the placeholder is defined as '' (empty), so it is NOT NULL and
-- has length 0. A NULL would show is_null=true and len=NULL.
-- begin-expected
-- columns: is_null | len
-- row: false, 0
-- end-expected
SELECT current_setting('custom.resid_l7', true) IS NULL AS is_null,
       length(current_setting('custom.resid_l7', true))::integer AS len;

-- ============================================================================
-- L8: bare CLUSTER after CLUSTER ... USING remembers the clustered index.
-- ============================================================================

DROP TABLE IF EXISTS resid_l8 CASCADE;
CREATE TABLE resid_l8 (id int PRIMARY KEY, v text);
CREATE INDEX resid_l8_idx ON resid_l8 (v);
CLUSTER resid_l8 USING resid_l8_idx;

-- bare CLUSTER must succeed (no 42704) now that the index is remembered
CLUSTER resid_l8;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP TABLE resid_l8;

-- ============================================================================
-- H36: SET SESSION AUTHORIZATION then RESET restores identity. Expressed in a
--      connection-user-agnostic form: after RESET, current_user = session_user.
-- ============================================================================

DROP ROLE IF EXISTS resid_h36;
CREATE ROLE resid_h36;
SET SESSION AUTHORIZATION resid_h36;

-- begin-expected
-- columns: cu | su
-- row: resid_h36, resid_h36
-- end-expected
SELECT current_user AS cu, session_user AS su;

RESET SESSION AUTHORIZATION;

-- after RESET, identity is restored (current_user matches session_user again,
-- and is no longer the scratch role)
-- begin-expected
-- columns: restored | not_scratch
-- row: true, true
-- end-expected
SELECT current_user = session_user AS restored,
       current_user <> 'resid_h36' AS not_scratch;

DROP ROLE IF EXISTS resid_h36;
