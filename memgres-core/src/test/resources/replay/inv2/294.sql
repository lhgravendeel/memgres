-- source: investigation-2026-08.md
-- finding: 294
-- title: ALTER DEFAULT PRIVILEGES is applied at exactly one site — the plain CREATE TABLE path — behind a guard that accepts only the TABLES object kind
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
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42P06
-- message-like: schema "zz_s" already exists
-- end-expected-error
CREATE SCHEMA zz_s;
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
