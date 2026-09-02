-- source: review-2026-08.md
-- finding: Root cause 4: the COMMENT grammar reads tokens until IS and the executor checks seven object kinds
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 4: the COMMENT grammar reads tokens until IS and the executor checks seven object kinds
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON CAST (int4 AS int8) IS 'c';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer #%^&* integer
-- end-expected-error
COMMENT ON OPERATOR #%^&* (int, int) IS 'nope';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "zz_nosuchcoll" for encoding "UTF8" does not exist
-- end-expected-error
COMMENT ON COLLATION zz_nosuchcoll IS 'nope';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: aggregate zz_nosuchagg(integer) does not exist
-- end-expected-error
COMMENT ON AGGREGATE zz_nosuchagg(int) IS 'nope';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
COMMENT ON TABLE zz_t IS 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "true"
-- end-expected-error
COMMENT ON TABLE zz_t IS true;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DEFAULT"
-- end-expected-error
COMMENT ON TABLE zz_t IS DEFAULT;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "||"
-- end-expected-error
COMMENT ON TABLE zz_t IS 'a' || 'b';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'y'"
-- end-expected-error
COMMENT ON TABLE zz_t IS 'x' 'y';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "N"
-- end-expected-error
COMMENT ON TABLE zz_t IS N'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
COMMENT ON TABLE zz_t, zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: column name must be qualified
-- end-expected-error
COMMENT ON COLUMN zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "zz_t"
-- end-expected-error
COMMENT ON zz_t IS 'x';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_t" is not a view
-- end-expected-error
COMMENT ON VIEW zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_t" is not an index
-- end-expected-error
COMMENT ON INDEX zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_t" is not a sequence
-- end-expected-error
COMMENT ON SEQUENCE zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_nosuchrel" does not exist
-- end-expected-error
COMMENT ON VIEW zz_nosuchrel IS 'x';
