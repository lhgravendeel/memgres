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

-- ============================================================================
-- SECTION F: a foreign table is a relation in the schema that names it
-- ============================================================================
-- It is created where the statement says, it holds that name there against every
-- other relation kind, the catalogs that list relations list it, reading one is
-- refused by its wrapper rather than called missing, and dropping the schema
-- takes it along.

DROP SCHEMA IF EXISTS r15_ns1 CASCADE;
DROP SCHEMA IF EXISTS r15_ns2 CASCADE;
DROP FOREIGN TABLE IF EXISTS r15_ft_pub;
DROP FOREIGN DATA WRAPPER IF EXISTS r15_fdw_ns CASCADE;
CREATE SCHEMA r15_ns1;
CREATE SCHEMA r15_ns2;
CREATE FOREIGN DATA WRAPPER r15_fdw_ns;
CREATE SERVER r15_srv_ns FOREIGN DATA WRAPPER r15_fdw_ns;
CREATE FOREIGN TABLE r15_ns1.r15_ft_ns (a int, b text) SERVER r15_srv_ns;

-- 14. created in the schema the statement names
-- begin-expected
-- columns: c
-- row: r15_ns1
-- end-expected
SELECT n.nspname AS c FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'r15_ft_ns';

-- 15. and it is a foreign table there
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_class WHERE relname = 'r15_ft_ns' AND relkind = 'f';

-- 16. information_schema gives it a table type of its own
-- begin-expected
-- columns: c
-- row: r15_ns1|FOREIGN
-- end-expected
SELECT table_schema || '|' || table_type AS c FROM information_schema.tables
  WHERE table_name = 'r15_ft_ns';

-- 17. and names the server behind it
-- begin-expected
-- columns: c
-- row: r15_ns1|r15_srv_ns
-- end-expected
SELECT foreign_table_schema || '|' || foreign_server_name AS c
  FROM information_schema.foreign_tables WHERE foreign_table_name = 'r15_ft_ns';

-- 18. its columns are described like any other relation's
-- begin-expected
-- columns: c
-- row: a|integer
-- row: b|text
-- end-expected
SELECT column_name || '|' || data_type AS c FROM information_schema.columns
  WHERE table_name = 'r15_ft_ns' ORDER BY ordinal_position;

-- 19. reading one is the wrapper's refusal, not a missing relation
-- begin-expected-error
-- sqlstate: 55000
-- message-like: has no handler
-- end-expected-error
SELECT * FROM r15_ns1.r15_ft_ns;

-- 20. so is writing one
-- begin-expected-error
-- sqlstate: 55000
-- message-like: has no handler
-- end-expected-error
INSERT INTO r15_ns1.r15_ft_ns VALUES (1, 'x');

-- 21. but the same name in another schema is not there at all
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: does not exist
-- end-expected-error
SELECT * FROM r15_ns2.r15_ft_ns;

-- 22. an unqualified foreign table is created in the first schema of the path
SET search_path = r15_ns2;
CREATE FOREIGN TABLE r15_ft_path (a int) SERVER r15_srv_ns;
SET search_path = r15_fdw, public;

-- begin-expected
-- columns: c
-- row: r15_ns2
-- end-expected
SELECT n.nspname AS c FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'r15_ft_path';

-- 23. a foreign table holds its name against every other relation kind
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: already exists
-- end-expected-error
CREATE TABLE r15_ns1.r15_ft_ns (a int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: already exists
-- end-expected-error
CREATE VIEW r15_ns1.r15_ft_ns AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: already exists
-- end-expected-error
CREATE FOREIGN TABLE r15_ns1.r15_ft_ns (a int) SERVER r15_srv_ns;

-- 24. and cannot take one held by a table, a view or a sequence
CREATE TABLE r15_ns1.r15_rel_t (a int);
CREATE VIEW r15_ns1.r15_rel_v AS SELECT 1 AS a;
CREATE SEQUENCE r15_ns1.r15_rel_s;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: already exists
-- end-expected-error
CREATE FOREIGN TABLE r15_ns1.r15_rel_t (a int) SERVER r15_srv_ns;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: already exists
-- end-expected-error
CREATE FOREIGN TABLE r15_ns1.r15_rel_v (a int) SERVER r15_srv_ns;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: already exists
-- end-expected-error
CREATE FOREIGN TABLE r15_ns1.r15_rel_s (a int) SERVER r15_srv_ns;

-- 24b. IF NOT EXISTS steps aside for whatever holds the name and changes nothing
CREATE FOREIGN TABLE IF NOT EXISTS r15_ns1.r15_ft_ns (c int) SERVER r15_srv_ns;
CREATE FOREIGN TABLE IF NOT EXISTS r15_ns1.r15_rel_t (c int) SERVER r15_srv_ns;

-- begin-expected
-- columns: c
-- row: a
-- row: b
-- end-expected
SELECT a.attname AS c FROM pg_attribute a JOIN pg_class r ON r.oid = a.attrelid
  WHERE r.relname = 'r15_ft_ns' AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;

-- begin-expected
-- columns: c
-- row: r
-- end-expected
SELECT relkind AS c FROM pg_class WHERE relname = 'r15_rel_t';

-- 25. a schema that is not there is refused before anything is created
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "r15_no_such" does not exist
-- end-expected-error
CREATE FOREIGN TABLE r15_no_such.r15_ft_x (a int) SERVER r15_srv_ns;

-- 26. one written into public is still in public
CREATE FOREIGN TABLE public.r15_ft_pub (a int) SERVER r15_srv_ns;

-- begin-expected
-- columns: c
-- row: public
-- end-expected
SELECT n.nspname AS c FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'r15_ft_pub';

-- 27. DROP FOREIGN TABLE honours the schema
-- begin-expected-error
-- sqlstate: 42704
-- message-like: does not exist
-- end-expected-error
DROP FOREIGN TABLE r15_ns2.r15_ft_pub;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_class WHERE relname = 'r15_ft_pub';

-- 28. dropping one that is not there, with and without IF EXISTS
-- begin-expected-error
-- sqlstate: 42704
-- message-like: does not exist
-- end-expected-error
DROP FOREIGN TABLE r15_ft_absent;

DROP FOREIGN TABLE IF EXISTS r15_ft_absent;

-- 29. the wrong kind of drop is refused in both directions
-- begin-expected-error
-- sqlstate: 42809
-- message-like: is not a table
-- end-expected-error
DROP TABLE r15_ns1.r15_ft_ns;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: is not a foreign table
-- end-expected-error
DROP FOREIGN TABLE r15_ns1.r15_rel_t;

-- 30. and neither statement removed anything
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::int AS c FROM pg_class
  WHERE relname IN ('r15_ft_ns', 'r15_rel_t');

-- 31. dropping it frees the name
DROP FOREIGN TABLE public.r15_ft_pub;
CREATE TABLE public.r15_ft_pub (a int);

-- begin-expected
-- columns: c
-- row: r
-- end-expected
SELECT relkind AS c FROM pg_class WHERE relname = 'r15_ft_pub';

DROP TABLE public.r15_ft_pub;

-- 32. dropping the schema takes its foreign tables with it
DROP SCHEMA r15_ns1 CASCADE;

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_class WHERE relname = 'r15_ft_ns';

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM information_schema.tables WHERE table_name = 'r15_ft_ns';

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM information_schema.columns WHERE table_name = 'r15_ft_ns';

DROP SCHEMA IF EXISTS r15_ns2 CASCADE;
DROP SERVER IF EXISTS r15_srv_ns CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS r15_fdw_ns CASCADE;
