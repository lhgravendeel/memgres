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

CREATE TABLE admin_t(id int PRIMARY KEY, note text);
INSERT INTO admin_t VALUES (1, 'x');

-- LISTEN / NOTIFY / UNLISTEN
LISTEN compat_chan;
NOTIFY compat_chan, 'hello';
UNLISTEN compat_chan;
LISTEN "MixedCase";
NOTIFY "MixedCase", 'msg';
UNLISTEN "MixedCase";
NOTIFY compat_chan, NULL;
NOTIFY compat_chan, repeat('x', 9000);

LISTEN "";
LISTEN "bad name";
LISTEN 123;
NOTIFY;
UNLISTEN no_such_chan, other;

-- LOCK / SET / SHOW / RESET
LOCK TABLE admin_t IN ROW EXCLUSIVE MODE;
LOCK TABLE admin_t IN ACCESS EXCLUSIVE MODE NOWAIT;
LOCK TABLE ONLY admin_t IN ACCESS SHARE MODE;
LOCK TABLE admin_t IN no_such_mode MODE;

SET application_name = 'compat';
SHOW application_name;
RESET application_name;
SET LOCAL application_name = 'localname';
SET TIME ZONE 'UTC';
SHOW search_path;
SET search_path = compat, pg_catalog;
SET search_path TO DEFAULT;
SET TIME ZONE 123456;
SET search_path = ;
SHOW ;
RESET ;

-- COMMENT ON more object kinds
CREATE SEQUENCE admin_seq START 1;
CREATE VIEW admin_v AS SELECT * FROM admin_t;
CREATE ROLE compat_role2;
COMMENT ON COLUMN admin_t.note IS 'column comment';
COMMENT ON VIEW admin_v IS 'view comment';
COMMENT ON SEQUENCE admin_seq IS 'seq comment';
COMMENT ON ROLE compat_role2 IS 'role comment';
COMMENT ON INDEX admin_t_pkey IS 'pk index';
COMMENT ON COLUMN admin_t.nope IS 'x';

-- EXPLAIN and COPY parsing
EXPLAIN (ANALYZE false, COSTS false, VERBOSE true) SELECT * FROM admin_t;
EXPLAIN (FORMAT text) SELECT * FROM admin_t;
EXPLAIN (FORMAT json) SELECT * FROM admin_t;
EXPLAIN (FORMAT yamlish) SELECT * FROM admin_t;
EXPLAIN (ANALYZE maybe) SELECT * FROM admin_t;
EXPLAIN (BUFFERS 123) SELECT * FROM admin_t;

COPY admin_t TO STDOUT;
COPY admin_t (id, note) TO STDOUT WITH (FORMAT csv);
COPY (SELECT * FROM admin_t) TO STDOUT;
COPY TO STDOUT;
COPY admin_t () TO STDOUT;
COPY admin_t TO STDOUT WITH (FORMAT no_such);

-- GRANT/REVOKE oddities
GRANT SELECT ON TABLE admin_t TO PUBLIC;
REVOKE ALL PRIVILEGES ON TABLE admin_t FROM PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE admin_seq TO compat_role2;
GRANT UPDATE(note) ON admin_t TO compat_role2;
GRANT SELECT(foo) ON TABLE admin_t TO compat_role2;
GRANT EXECUTE ON TABLE admin_t TO compat_role2;
GRANT USAGE ON TABLE admin_t TO compat_role2;
REVOKE SELECT ON no_such FROM compat_role2;

DROP ROLE compat_role2;
DROP SCHEMA compat CASCADE;
