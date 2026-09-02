-- source: investigation.md
-- finding: 8
-- title: Trigger creation is unvalidated
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER a AFTER UPDATE ON t ...;
-- name already exists on t
--   PG: 42710 | mg: OK (silently replaces or duplicates)
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER b AFTER INSERT ON t FOR EACH ROW WHEN (OLD.v > 1) ...;
--   PG: 42P17 INSERT trigger's WHEN cannot reference OLD | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER c AFTER DELETE ON t FOR EACH ROW WHEN (NEW.v > 1) ...;
-- PG: 42P17 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER d AFTER INSERT ON t FOR EACH STATEMENT WHEN (NEW.v > 1) ...;
-- PG: 42P17 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER e BEFORE INSERT ON t REFERENCING NEW TABLE AS nt ...;
--   PG: 42P17 transition table only for AFTER | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER f AFTER TRUNCATE ON t FOR EACH ROW ...;
-- PG: 0A000 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER g INSTEAD OF INSERT ON v FOR EACH STATEMENT ...;
-- PG: 0A000 must be FOR EACH ROW | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE TRIGGER h INSTEAD OF UPDATE OF id ON v FOR EACH ROW ...;
-- PG: 0A000 no column lists | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "BEFORE"
-- end-expected-error
CREATE CONSTRAINT TRIGGER i BEFORE INSERT ...;
-- PG: 42601 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
CREATE CONSTRAINT TRIGGER j AFTER INSERT ... FOR EACH STATEMENT ...;
-- PG: 42601 | mg: OK;
