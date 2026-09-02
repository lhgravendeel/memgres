-- source: investigation.md
-- finding: 120
-- title: `ADD COLUMN` stores a default that violates the column's own type ⚠️
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE t (i int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO t VALUES (1);
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(2)
-- end-expected-error
ALTER TABLE t ADD COLUMN c varchar(2) DEFAULT 'abcdef';
--   PG: 22001 value too long for type character varying(2)
--   mg: succeeds; SELECT c returns 'abcdef';
