-- ============================================================================
-- Feature Comparison: Round 15 — Foreign Data Wrappers
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_fdw CASCADE;
CREATE SCHEMA r15_fdw;
SET search_path = r15_fdw, public;

-- ============================================================================
-- SECTION A: CREATE/DROP FOREIGN DATA WRAPPER
-- ============================================================================

CREATE FOREIGN DATA WRAPPER r15_fdw_basic;

-- 1. Visible in pg_foreign_data_wrapper
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_foreign_data_wrapper WHERE fdwname='r15_fdw_basic';

CREATE FOREIGN DATA WRAPPER r15_fdw_opts OPTIONS (debug 'true', retries '3');

-- 2. Options stored
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_foreign_data_wrapper
  WHERE fdwname='r15_fdw_opts' AND fdwoptions IS NOT NULL;

CREATE FOREIGN DATA WRAPPER r15_fdw_drop;
DROP FOREIGN DATA WRAPPER r15_fdw_drop;

-- 3. Dropped
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_foreign_data_wrapper WHERE fdwname='r15_fdw_drop';

-- ============================================================================
-- SECTION B: CREATE SERVER
-- ============================================================================

CREATE FOREIGN DATA WRAPPER r15_fdw_s;
CREATE SERVER r15_srv FOREIGN DATA WRAPPER r15_fdw_s;

-- 4. Server created
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_foreign_server WHERE srvname='r15_srv';

CREATE FOREIGN DATA WRAPPER r15_fdw_so;
CREATE SERVER r15_srv_opts FOREIGN DATA WRAPPER r15_fdw_so
  OPTIONS (host 'localhost', port '5432', dbname 'foo');

-- 5. Server options stored
SELECT count(*)::int AS c FROM pg_foreign_server
  WHERE srvname='r15_srv_opts' AND srvoptions IS NOT NULL;

CREATE FOREIGN DATA WRAPPER r15_fdw_a;
CREATE SERVER r15_srv_a FOREIGN DATA WRAPPER r15_fdw_a OPTIONS (host 'h1');
ALTER SERVER r15_srv_a OPTIONS (SET host 'h2', ADD port '5432');

-- 6. ALTER SERVER accepted
SELECT count(*)::int AS c FROM pg_foreign_server WHERE srvname='r15_srv_a';

-- ============================================================================
-- SECTION C: CREATE USER MAPPING
-- ============================================================================

CREATE FOREIGN DATA WRAPPER r15_fdw_um;
CREATE SERVER r15_srv_um FOREIGN DATA WRAPPER r15_fdw_um;
CREATE USER MAPPING FOR CURRENT_USER SERVER r15_srv_um OPTIONS (user 'u', password 'p');

-- 7. pg_user_mapping populated
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_user_mapping m
  JOIN pg_foreign_server s ON m.umserver = s.oid
  WHERE s.srvname='r15_srv_um';

CREATE FOREIGN DATA WRAPPER r15_fdw_pu;
CREATE SERVER r15_srv_pu FOREIGN DATA WRAPPER r15_fdw_pu;
CREATE USER MAPPING FOR PUBLIC SERVER r15_srv_pu;

-- 8. pg_user_mappings view
SELECT count(*)::int AS c FROM pg_user_mappings m WHERE m.srvname='r15_srv_pu';

-- ============================================================================
-- SECTION D: CREATE FOREIGN TABLE
-- ============================================================================

CREATE FOREIGN DATA WRAPPER r15_fdw_ft;
CREATE SERVER r15_srv_ft FOREIGN DATA WRAPPER r15_fdw_ft;
CREATE FOREIGN TABLE r15_ft_t (id int, name text)
  SERVER r15_srv_ft OPTIONS (schema_name 'public', table_name 't');

-- 9. pg_foreign_table populated
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_foreign_table ft
  JOIN pg_class c ON ft.ftrelid = c.oid
  WHERE c.relname='r15_ft_t';

CREATE FOREIGN DATA WRAPPER r15_fdw_rk;
CREATE SERVER r15_srv_rk FOREIGN DATA WRAPPER r15_fdw_rk;
CREATE FOREIGN TABLE r15_ft_rk (id int) SERVER r15_srv_rk;

-- 10. pg_class.relkind='f'
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_class WHERE relname='r15_ft_rk' AND relkind='f';

CREATE FOREIGN DATA WRAPPER r15_fdw_cols;
CREATE SERVER r15_srv_cols FOREIGN DATA WRAPPER r15_fdw_cols;
CREATE FOREIGN TABLE r15_ft_cols (id int, nm text) SERVER r15_srv_cols;

-- 11. Columns in pg_attribute
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::int AS c FROM pg_attribute a
  JOIN pg_class c ON a.attrelid = c.oid
  WHERE c.relname='r15_ft_cols' AND NOT a.attisdropped AND a.attnum > 0;

-- ============================================================================
-- SECTION E: IMPORT FOREIGN SCHEMA (syntax)
-- ============================================================================

CREATE FOREIGN DATA WRAPPER r15_fdw_imp;
CREATE SERVER r15_srv_imp FOREIGN DATA WRAPPER r15_fdw_imp;
CREATE SCHEMA r15_imp_target;

-- 12. IMPORT FOREIGN SCHEMA parses
IMPORT FOREIGN SCHEMA public FROM SERVER r15_srv_imp INTO r15_imp_target;

-- 13. With LIMIT TO
CREATE FOREIGN DATA WRAPPER r15_fdw_l;
CREATE SERVER r15_srv_l FOREIGN DATA WRAPPER r15_fdw_l;
CREATE SCHEMA r15_imp_l;

IMPORT FOREIGN SCHEMA public LIMIT TO (t1, t2)
  FROM SERVER r15_srv_l INTO r15_imp_l;
