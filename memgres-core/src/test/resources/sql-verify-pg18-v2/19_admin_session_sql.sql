\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

-- administrative/session-syntax oriented coverage
SHOW TimeZone;
SET work_mem = '4MB';
SHOW work_mem;
RESET work_mem;
SET LOCAL work_mem = '8MB';

LISTEN compat_chan;
NOTIFY compat_chan, 'hello';
UNLISTEN compat_chan;
UNLISTEN *;

CREATE TABLE admin_t(id int PRIMARY KEY, note text);
INSERT INTO admin_t VALUES (1, 'x'), (2, 'y');

EXPLAIN SELECT * FROM admin_t WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM admin_t ORDER BY id;
EXPLAIN ANALYZE SELECT * FROM admin_t WHERE id = 1;

ANALYZE admin_t;
VACUUM admin_t;

CREATE VIEW admin_v AS SELECT * FROM admin_t;
ALTER VIEW admin_v RENAME COLUMN note TO note2;
ALTER VIEW admin_v SET (security_barrier=true);
ALTER VIEW admin_v RESET (security_barrier);

CREATE SEQUENCE admin_seq START 5 INCREMENT 2;
SELECT nextval('admin_seq'), currval('admin_seq');
ALTER SEQUENCE admin_seq RESTART WITH 100;
ALTER SEQUENCE admin_seq INCREMENT BY 5;
SELECT nextval('admin_seq');

CREATE DOMAIN admin_dom AS int CHECK (VALUE > 0);
ALTER DOMAIN admin_dom SET DEFAULT 1;
ALTER DOMAIN admin_dom DROP DEFAULT;
ALTER DOMAIN admin_dom ADD CONSTRAINT admin_dom_ck CHECK (VALUE < 100);
ALTER DOMAIN admin_dom DROP CONSTRAINT admin_dom_ck;

CREATE TYPE admin_enum AS ENUM ('a', 'b');
ALTER TYPE admin_enum ADD VALUE 'c';

CREATE MATERIALIZED VIEW admin_mv AS SELECT * FROM admin_t;
CREATE UNIQUE INDEX admin_mv_uq ON admin_mv(id);
REFRESH MATERIALIZED VIEW admin_mv;
REFRESH MATERIALIZED VIEW CONCURRENTLY admin_mv;

REINDEX TABLE admin_t;
CLUSTER admin_t USING admin_t_pkey;

LOCK TABLE admin_t IN ACCESS SHARE MODE;

-- role/privilege syntax
CREATE ROLE compat_role;
GRANT SELECT, INSERT ON admin_t TO compat_role;
REVOKE INSERT ON admin_t FROM compat_role;
GRANT USAGE ON SCHEMA compat TO compat_role;
REVOKE USAGE ON SCHEMA compat FROM compat_role;
ALTER ROLE compat_role SET work_mem = '1MB';
SET ROLE compat_role;
RESET ROLE;
DROP ROLE compat_role;

-- bad admin/session cases
SET no_such_guc = '1';
SHOW no_such_guc;
LISTEN;
NOTIFY;
UNLISTEN no_such_chan, other;
EXPLAIN ();
VACUUM (NOTANOPTION) admin_t;
ALTER VIEW no_such_view RENAME COLUMN a TO b;
ALTER SEQUENCE no_such_seq RESTART WITH 1;
ALTER DOMAIN no_such_domain SET DEFAULT 1;
ALTER TYPE no_such_type ADD VALUE 'x';
REFRESH MATERIALIZED VIEW CONCURRENTLY no_such_mv;
LOCK TABLE no_such IN ACCESS SHARE MODE;
GRANT SELECT ON no_such TO compat_role;
SET ROLE no_such_role;
DROP ROLE no_such_role;

DROP SCHEMA compat CASCADE;
