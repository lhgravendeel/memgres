-- source: investigation-2026-08.md
-- finding: 323
-- title: The EXPLAIN payload is parsed by the general statement parser with no explainable-statement gate, and the resulting ExplainStmt wrapper then defeats a check tha
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "int"
-- end-expected-error
EXPLAIN CREATE TABLE zz_nope (x int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DROP"
-- end-expected-error
EXPLAIN DROP TABLE zz_nope;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SET"
-- end-expected-error
EXPLAIN SET work_mem = '4MB';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CHECKPOINT"
-- end-expected-error
EXPLAIN CHECKPOINT;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
EXPLAIN DO $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "GRANT"
-- end-expected-error
EXPLAIN GRANT SELECT ON pg_class TO PUBLIC;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "COPY"
-- end-expected-error
EXPLAIN COPY pg_class TO STDOUT;
