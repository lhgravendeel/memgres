CREATE ROLE ze_r1 NOLOGIN;
CREATE ROLE ze_r2 NOLOGIN;
CREATE TABLE ze_t (a int);
ALTER TABLE ze_t OWNER TO ze_r1;
-- begin-expected
-- columns: n
-- row: ze_r1
-- end-expected
SELECT pg_get_userbyid((SELECT oid FROM pg_roles WHERE rolname='ze_r1')) AS n;
-- begin-expected
-- columns: n
-- row: ze_r2
-- end-expected
SELECT pg_get_userbyid((SELECT oid FROM pg_roles WHERE rolname='ze_r2')) AS n;
-- begin-expected
-- columns: o
-- row: ze_r1
-- end-expected
SELECT tableowner::text AS o FROM pg_tables WHERE tablename='ze_t';
-- begin-expected
-- columns: r
-- row: ze_r1
-- end-expected
SELECT (SELECT oid FROM pg_roles WHERE rolname='ze_r1')::regrole::text AS r;
-- begin-expected
-- columns: same
-- row: true
-- end-expected
SELECT ('ze_r2'::regrole::oid = (SELECT oid FROM pg_roles WHERE rolname='ze_r2'))::text AS same;
-- begin-expected
-- columns: p
-- row: false
-- end-expected
SELECT has_table_privilege('ze_r2', 'ze_t', 'SELECT')::text AS p;
DROP TABLE ze_t;
DROP ROLE ze_r1, ze_r2;
CREATE SCHEMA ze_s;
CREATE TABLE ze_s.ze_u (a int);
-- begin-expected
-- columns: missing
-- row: true
-- end-expected
SELECT (to_regclass('ze_u') IS NULL)::text AS missing;
SET search_path = ze_s, public;
-- begin-expected
-- columns: missing
-- row: false
-- end-expected
SELECT (to_regclass('ze_u') IS NULL)::text AS missing;
-- begin-expected
-- columns: s
-- row: ze_s
-- end-expected
SELECT current_schema()::text AS s;
SET search_path = ze_nosuch;
-- begin-expected
-- columns: none
-- row: true
-- end-expected
SELECT (current_schema() IS NULL)::text AS none;
SET search_path = public;
DROP SCHEMA ze_s CASCADE;
CREATE TABLE ze_w (a int, g int);
INSERT INTO ze_w VALUES (1,1),(2,1),(3,2);
-- begin-expected
-- columns: r
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT pg_catalog.row_number() OVER (ORDER BY a) AS r FROM ze_w;
-- begin-expected
-- columns: r
-- row: 1
-- row: 2
-- row: 1
-- end-expected
SELECT pg_catalog.rank() OVER (PARTITION BY g ORDER BY a) AS r FROM ze_w;
-- begin-expected
-- columns: c
-- row: 3
-- row: 3
-- row: 3
-- end-expected
SELECT pg_catalog.count(*) OVER () AS c FROM ze_w;
DROP TABLE ze_w;
-- begin-expected
-- columns: t
-- row: 2020-01-01 17:30:00+00
-- end-expected
SELECT (('2020-01-01 12:00:00'::timestamp) AT TIME ZONE '+05:30')::text AS t;
DISCARD SEQUENCES;
DISCARD TEMPORARY;
DISCARD PLANS;
DISCARD ALL;
