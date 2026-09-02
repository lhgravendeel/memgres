-- source: review-2026-08.md
-- finding: Root cause 2: default privileges are applied at exactly one site, the plain CREATE TABLE path
-- area: Ownership, default privileges and role membership
-- title: Root cause 2: default privileges are applied at exactly one site, the plain CREATE TABLE path
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_s.v AS SELECT 1 AS i;
-- begin-expected
-- ok: 1
-- end-expected
CREATE MATERIALIZED VIEW zz_s.mv AS SELECT 1 AS i;
-- begin-expected
-- ok: 1
-- end-expected
CREATE TABLE zz_s.cta AS SELECT 1 AS i;
-- begin-expected
-- columns: has_table_privilege:text | has_table_privilege:text | has_table_privilege:text
-- row: true | true | true
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_a','zz_s.v','SELECT')::text,
       has_table_privilege('zz_a','zz_s.mv','SELECT')::text,
       has_table_privilege('zz_a','zz_s.cta','SELECT')::text;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT USAGE ON SEQUENCES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s.q;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s.ser (id serial);
-- begin-expected
-- columns: has_sequence_privilege:text | has_sequence_privilege:text
-- row: true | true
-- rowcount: 1
-- end-expected
SELECT has_sequence_privilege('zz_a','zz_s.q','USAGE')::text,
       has_sequence_privilege('zz_a','zz_s.ser_id_seq','USAGE')::text;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT EXECUTE ON FUNCTIONS TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_s.f() RETURNS int LANGUAGE sql AS 'SELECT 1';
-- begin-expected
-- columns: has_function_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_function_privilege('zz_a','zz_s.f()','EXECUTE')::text;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES GRANT USAGE ON SCHEMAS TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s2;
-- begin-expected
-- columns: has_schema_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_schema_privilege('zz_a','zz_s2','USAGE')::text;
