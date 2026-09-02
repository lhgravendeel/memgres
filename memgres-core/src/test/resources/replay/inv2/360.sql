-- source: investigation-2026-08.md
-- finding: 360
-- title: TABLESAMPLE reads its arguments and its relation without checking either: the percentage via executor.toDouble with no null or type check (and the range test at
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ts (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_ts VALUES (1),(2),(3);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_tsv AS SELECT id FROM zz_vf2_ts;
-- begin-expected
-- ok: 3
-- end-expected
CREATE MATERIALIZED VIEW zz_vf2_tsm AS SELECT id FROM zz_vf2_ts;
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_tsm TABLESAMPLE BERNOULLI (100);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: TABLESAMPLE clause can only be applied to tables and materialized views
-- end-expected-error
SELECT count(*) FROM zz_vf2_tsv TABLESAMPLE BERNOULLI (100);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_tp (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_tp VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 2202H
-- message-like: TABLESAMPLE parameter cannot be null
-- end-expected-error
SELECT count(*) FROM zz_vf2_tp TABLESAMPLE BERNOULLI (NULL);
-- begin-expected-error
-- sqlstate: 2202H
-- message-like: sample percentage must be between 0 and 100
-- end-expected-error
SELECT count(*) FROM zz_vf2_tp TABLESAMPLE BERNOULLI ('nan'::float8);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of TABLESAMPLE must be type real, not type boolean
-- end-expected-error
SELECT count(*) FROM zz_vf2_tp TABLESAMPLE BERNOULLI (true);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of TABLESAMPLE must be type real, not type text
-- end-expected-error
SELECT count(*) FROM zz_vf2_tp TABLESAMPLE BERNOULLI ('100'::text);
-- begin-expected-error
-- sqlstate: 2202H
-- message-like: tablesample method bernoulli requires 1 argument, not 2
-- end-expected-error
SELECT count(*) FROM zz_vf2_tp TABLESAMPLE BERNOULLI (50, 50);
