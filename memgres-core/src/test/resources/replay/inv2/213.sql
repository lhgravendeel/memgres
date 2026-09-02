-- source: investigation-2026-08.md
-- finding: 213
-- title: MERGE's own code path has no trigger dispatch. executeMergeInner resolves one Table and works from targetTable.getRows(); the August report established that it 
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_b" does not exist
-- end-expected-error
CREATE VIEW zz_v AS SELECT id, v FROM zz_b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_v" does not exist
-- end-expected-error
CREATE TRIGGER zz_t INSTEAD OF INSERT OR UPDATE OR DELETE ON zz_v FOR EACH ROW EXECUTE FUNCTION zz_f();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_v" does not exist
-- end-expected-error
MERGE INTO zz_v t USING (SELECT 1 AS id, 9 AS v) s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_lg" does not exist
-- end-expected-error
SELECT t FROM zz_lg ORDER BY seq;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_b" does not exist
-- end-expected-error
SELECT id, v FROM zz_b ORDER BY id;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE TRIGGER zz_b BEFORE INSERT OR UPDATE OR DELETE ON zz_t FOR EACH ROW EXECUTE FUNCTION zz_f();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE TRIGGER zz_a AFTER INSERT OR UPDATE OR DELETE ON zz_t FOR EACH ROW EXECUTE FUNCTION zz_f();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE TRIGGER zz_s AFTER INSERT OR UPDATE OR DELETE ON zz_t FOR EACH STATEMENT EXECUTE FUNCTION zz_f();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
MERGE INTO zz_t t USING (VALUES (2)) AS s(id) ON t.id = s.id WHEN MATCHED THEN DELETE;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_lg" does not exist
-- end-expected-error
SELECT t FROM zz_lg ORDER BY seq;
-- session B: BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM zz_r2_mvcc_t;
-- session A: INSERT INTO zz_r2_mvcc_t VALUES (4,40,'d');
-- session B:
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r2_mvcc_t" does not exist
-- end-expected-error
MERGE INTO zz_r2_mvcc_t t USING (SELECT 4 AS k) x ON t.i = x.k
  WHEN MATCHED THEN UPDATE SET v = 7
  WHEN NOT MATCHED THEN INSERT (i,v,s) VALUES (4,44,'m');
