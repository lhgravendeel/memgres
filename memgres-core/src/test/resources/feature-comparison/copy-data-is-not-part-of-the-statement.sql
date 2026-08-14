-- ============================================================================
-- The data of a COPY FROM STDIN is not part of the statement
--
-- The rows of a COPY FROM STDIN never reach the server inside the statement.
-- The lines that follow it in a script are read by the client and turned into
-- the copy's own messages, so a server handed one of them as SQL answers for
-- the first word of it -- a data line is a statement like any other, and not
-- one the grammar knows.
--
-- A client that opens the copy and then sends nothing is told the copy failed,
-- and the relation and the column list are still resolved before the copy is
-- opened at all.
--
-- What a refused row carries -- the failing row in the error's DETAIL and the
-- line of the input in its CONTEXT -- wants a copy stream to raise it, and a
-- statement holding data lines cannot be written here, so it lives in
-- PartitionTriggersAndMergeConcurrencyTest instead.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE zzcp_d (i int, k int);

-- ============================================================================
-- A data line handed to the server as SQL
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
1	2;

-- the same line as a CSV copy would have sent it
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "5"
-- end-expected-error
5,6;

-- and one carrying a quoted field
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
1,"a,b";

-- ============================================================================
-- A copy nobody sends to
-- ============================================================================

-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed
-- end-expected-error
COPY zzcp_d FROM STDIN;

-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed
-- end-expected-error
COPY zzcp_d (i,k) FROM STDIN;

-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed
-- end-expected-error
COPY zzcp_d FROM STDIN WITH (FORMAT csv, HEADER);

-- ============================================================================
-- What is settled before the copy is opened
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzcp_nosuch" does not exist
-- end-expected-error
COPY zzcp_nosuch FROM STDIN;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zzcp_d" does not exist
-- end-expected-error
COPY zzcp_d (i,nosuch) FROM STDIN;

-- Nothing any of that did left a row behind, and the relation still takes one.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzcp_d;

INSERT INTO zzcp_d VALUES (1,2);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zzcp_d;

-- teardown
DROP TABLE zzcp_d;
