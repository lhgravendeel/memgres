-- source: review-2026-08.md
-- finding: Root cause 1: MERGE is a second, thinner DML implementation with none of the guards
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 1: MERGE is a second, thinner DML implementation with none of the guards
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s (id int primary key, n int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_s VALUES (1,10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_s TO zz_r;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_r;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for table zz_s
-- end-expected-error
MERGE INTO zz_s t USING (VALUES (1)) AS v(id) ON t.id = v.id
  WHEN MATCHED THEN UPDATE SET n = 99;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ga (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_ga VALUES (1, 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_gav AS SELECT i, v FROM zz_ga WHERE v < 10 WITH CHECK OPTION;
-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zz_gav"
-- end-expected-error
MERGE INTO zz_gav t USING (VALUES (1, 90)) s(i, v) ON t.i = s.i
  WHEN MATCHED THEN UPDATE SET v = s.v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g8 (id int, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_g8 VALUES (1, 1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "WHERE"
-- end-expected-error
MERGE INTO zz_g8 t USING (VALUES (1, 2)) s(id, v) ON t.id = s.id
  WHEN MATCHED THEN DELETE WHERE t.id = 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (id int PRIMARY KEY, v text) PARTITION BY RANGE (id);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pa PARTITION OF zz_p FOR VALUES FROM (0) TO (10);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p VALUES (1, 'a');
-- begin-expected
-- columns: merge_action:text | id:int4 | v:text
-- row: UPDATE | 1 | n
-- rowcount: 1
-- end-expected
MERGE INTO zz_p t USING (VALUES (1, 'n')) s(id, v) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v RETURNING merge_action(), t.id, t.v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g3 (id int, v int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: INSERT has more target columns than expressions
-- end-expected-error
MERGE INTO zz_g3 t USING (VALUES (1, 2)) s(id, v) ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (1,1);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN READ ONLY;
-- begin-expected-error
-- sqlstate: 25006
-- message-like: cannot execute MERGE in a read-only transaction
-- end-expected-error
MERGE INTO zz_t t USING (SELECT 1 AS i) s ON t.i = s.i
  WHEN MATCHED THEN UPDATE SET v = 42;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g7 (id int, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_g7 VALUES (1, 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_g7_r AS ON UPDATE TO zz_g7 DO ALSO NOTHING;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot execute MERGE on relation "zz_g7"
-- end-expected-error
MERGE INTO zz_g7 t USING (VALUES (1, 2)) s(id, v) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g9 (id int, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_g9 VALUES (1, 1);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
MERGE INTO zz_g9 t USING (VALUES (2, 2)) s(id, v) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = nosuchcol;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g4 (id int, v int, w text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_g4 VALUES (1, 10, 'a');
-- begin-expected
-- columns: id:int4 | v:int4 | id:int4 | v:int4 | w:text
-- row: 1 | 100 | 1 | 100 | a
-- rowcount: 1
-- end-expected
MERGE INTO zz_g4 t USING (VALUES (1, 100)) s(id, v) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v RETURNING *;
-- begin-expected
-- columns: id:int4 | v:int4
-- row: 1 | 200
-- rowcount: 1
-- end-expected
MERGE INTO zz_g4 t USING (VALUES (1, 200)) s(id, v) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v RETURNING s.*;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_g5" does not exist
-- end-expected-error
MERGE INTO ONLY zz_g5 t USING (VALUES (1, 2)) s(id, v) ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v RETURNING t.id, t.v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g6 (i int GENERATED ALWAYS AS IDENTITY, v int);
-- begin-expected
-- columns: i:serial
-- row: 5
-- rowcount: 1
-- end-expected
MERGE INTO zz_g6 t USING (VALUES (5, 50)) s(i, v) ON t.i = s.i
  WHEN NOT MATCHED THEN INSERT (i, v) OVERRIDING SYSTEM VALUE VALUES (s.i, s.v) RETURNING t.i;
