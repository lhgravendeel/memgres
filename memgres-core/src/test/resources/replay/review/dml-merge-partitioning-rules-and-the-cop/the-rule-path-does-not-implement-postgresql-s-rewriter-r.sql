-- source: review-2026-08.md
-- finding: The rule path does not implement PostgreSQL's rewriter restrictions
-- area: DML, MERGE, partitioning, rules and the COPY/extended-protocol surface
-- title: The rule path does not implement PostgreSQL's rewriter restrictions
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_r5 (i int primary key, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_r5v AS SELECT i, v FROM zz_vf_r5;
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_vf_r5_r AS ON INSERT TO zz_vf_r5v DO INSTEAD INSERT INTO zz_vf_r5 VALUES (NEW.i, NEW.v);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot perform INSERT RETURNING on relation "zz_vf_r5v"
-- end-expected-error
INSERT INTO zz_vf_r5v VALUES (1,'a') RETURNING i;
-- begin-expected
-- columns: i:int4 | v:text
-- rowcount: 0
-- end-expected
SELECT i, v FROM zz_vf_r5;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_r6 (i int primary key, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_r6log (m text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_vf_r6_h AS ON INSERT TO zz_vf_r6 DO ALSO INSERT INTO zz_vf_r6log VALUES ('i');
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules
-- end-expected-error
INSERT INTO zz_vf_r6 VALUES (1,'a') ON CONFLICT (i) DO NOTHING;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot execute MERGE on relation "zz_vf_r6"
-- end-expected-error
MERGE INTO zz_vf_r6 t USING (SELECT 1 AS i) s ON t.i=s.i WHEN NOT MATCHED THEN DO NOTHING;
