-- ============================================================================
-- What a refusal writes the row as, and which direction a COPY reads
--
-- Every value PostgreSQL prints inside a DETAIL is written by the column's own
-- output function -- the string a query would have returned. An array reads in
-- braces, a boolean as one letter, a bytea in hex, a timestamp with a space
-- between its date and its time. That holds wherever a refusal prints a row or
-- a key: a CHECK, a NOT NULL, a unique index, a foreign key, an exclusion
-- constraint, a partition constraint, a row no partition will take, and a
-- view's check option all write their values the same way.
--
-- The direction words are the other half. STDIN and STDOUT are one thing to
-- PostgreSQL's grammar -- the absence of a file name -- and FROM or TO alone
-- decides which way the rows travel, so COPY ... FROM STDOUT reads from the
-- client and COPY ... TO STDIN writes to it.
--
-- The DETAIL each refusal carries is asserted through JDBC in
-- PartitionTriggersAndMergeConcurrencyTest; what stands here is the statement,
-- its state and its message.
-- ============================================================================

-- ============================================================================
-- A CHECK prints the whole row, and an array in it reads as an array
-- ============================================================================
CREATE TABLE frw_ck (i int, a int[], CONSTRAINT frw_ck_ck CHECK (i < 10));

-- DETAIL: Failing row contains (50, {1,2}).
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "frw_ck" violates check constraint "frw_ck_ck"
-- end-expected-error
INSERT INTO frw_ck VALUES (50, '{1,2}');

-- DETAIL: Failing row contains (50, {}).
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "frw_ck" violates check constraint "frw_ck_ck"
-- end-expected-error
INSERT INTO frw_ck VALUES (50, '{}');

-- DETAIL: Failing row contains (50, null).
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "frw_ck" violates check constraint "frw_ck_ck"
-- end-expected-error
INSERT INTO frw_ck VALUES (50, NULL);

-- DETAIL: Failing row contains (50, {{1,2},{3,4}}).
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "frw_ck" violates check constraint "frw_ck_ck"
-- end-expected-error
INSERT INTO frw_ck VALUES (50, '{{1,2},{3,4}}');

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM frw_ck;

DROP TABLE frw_ck;

-- ============================================================================
-- Every other type whose Java rendering is not PostgreSQL's, in one row
-- ============================================================================
CREATE TABLE frw_wt (i int, b bool, y bytea, t time, d date, s timestamp, f float8, r real, n numeric, a text[], CONSTRAINT frw_wt_ck CHECK (i < 10));

-- DETAIL: Failing row contains (50, t, \x0102, 03:04:00, 2020-01-02,
--         2020-01-02 03:04:05, 1, 2, 1.50, {a,"b c"}).
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "frw_wt" violates check constraint "frw_wt_ck"
-- end-expected-error
INSERT INTO frw_wt VALUES (50, true, '\x0102', '03:04:00', '2020-01-02', '2020-01-02 03:04:05', 1.0, 2.0, 1.50, '{a,"b c"}');

DROP TABLE frw_wt;

-- ============================================================================
-- A NOT NULL prints the same row the same way, and the empty column as null
-- ============================================================================
CREATE TABLE frw_nn (i int, a int[], k int NOT NULL);

-- DETAIL: Failing row contains (1, {3,4}, null).
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "k" of relation "frw_nn" violates not-null constraint
-- end-expected-error
INSERT INTO frw_nn VALUES (1, '{3,4}', NULL);

DROP TABLE frw_nn;

-- ============================================================================
-- A key is written the same way a row is, however many columns it spans
-- ============================================================================
CREATE TABLE frw_ux (a int[] PRIMARY KEY);
INSERT INTO frw_ux VALUES ('{1,2}');

-- DETAIL: Key (a)=({1,2}) already exists.
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "frw_ux_pkey"
-- end-expected-error
INSERT INTO frw_ux VALUES ('{1,2}');

DROP TABLE frw_ux;

CREATE TABLE frw_kk (b bool, y bytea, s timestamp, n numeric, f float8, a text[], PRIMARY KEY (b,y,s,n,f,a));
INSERT INTO frw_kk VALUES (true, '\x0102', '2020-01-02 03:04:05', 1.50, 1.0, '{a,"b c"}');

-- DETAIL: Key (b, y, s, n, f, a)=(t, \x0102, 2020-01-02 03:04:05, 1.50, 1,
--         {a,"b c"}) already exists.
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "frw_kk_pkey"
-- end-expected-error
INSERT INTO frw_kk VALUES (true, '\x0102', '2020-01-02 03:04:05', 1.50, 1.0, '{a,"b c"}');

DROP TABLE frw_kk;

CREATE TABLE frw_fp (b bool, a int[], PRIMARY KEY (b,a));
CREATE TABLE frw_fc (b bool, a int[], FOREIGN KEY (b,a) REFERENCES frw_fp);

-- DETAIL: Key (b, a)=(f, {7,8}) is not present in table "frw_fp".
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "frw_fc" violates foreign key constraint
-- end-expected-error
INSERT INTO frw_fc VALUES (false, '{7,8}');

DROP TABLE frw_fc;
DROP TABLE frw_fp;

CREATE TABLE frw_ex (a int[], EXCLUDE (a WITH =));
INSERT INTO frw_ex VALUES ('{1,2}');

-- DETAIL: Key (a)=({1,2}) conflicts with existing key (a)=({1,2}).
-- begin-expected-error
-- sqlstate: 23P01
-- message-like: conflicting key value violates exclusion constraint "frw_ex_a_excl"
-- end-expected-error
INSERT INTO frw_ex VALUES ('{1,2}');

DROP TABLE frw_ex;

-- ============================================================================
-- A partition writes its key and its row the same way
-- ============================================================================
CREATE TABLE frw_pa (a int[], k int) PARTITION BY RANGE (a);
CREATE TABLE frw_pa0 PARTITION OF frw_pa FOR VALUES FROM ('{0}') TO ('{9}');

-- DETAIL: Partition key of the failing row contains (a) = ({50,2}).
-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation "frw_pa" found for row
-- end-expected-error
INSERT INTO frw_pa VALUES ('{50,2}', 1);

DROP TABLE frw_pa CASCADE;

CREATE TABLE frw_pp (i int, a int[]) PARTITION BY RANGE (i);
CREATE TABLE frw_pp0 PARTITION OF frw_pp FOR VALUES FROM (0) TO (10);

-- DETAIL: Failing row contains (50, {1,2}).
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "frw_pp0" violates partition constraint
-- end-expected-error
INSERT INTO frw_pp0 VALUES (50, '{1,2}');

DROP TABLE frw_pp CASCADE;

-- ============================================================================
-- A view's check option prints the row it would have written
-- ============================================================================
CREATE TABLE frw_vt (i int, a int[]);
CREATE VIEW frw_vv AS SELECT * FROM frw_vt WHERE i < 10 WITH CHECK OPTION;

-- DETAIL: Failing row contains (50, {1,2}).
-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "frw_vv"
-- end-expected-error
INSERT INTO frw_vv VALUES (50, '{1,2}');

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM frw_vt;

DROP VIEW frw_vv;
DROP TABLE frw_vt;

-- ============================================================================
-- The direction is read from the FROM or the TO alone
--
-- A plain statement cannot send a copy's data, so the client gives up on a
-- copy in the moment the server asks for it -- and it asks for it just the
-- same when the statement said STDOUT. A copy out is refused on the client
-- side before it is read, whether the statement said STDOUT or STDIN.
-- ============================================================================
CREATE TABLE frw_d (i int, k int);

-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed
-- end-expected-error
COPY frw_d FROM STDIN;

-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed
-- end-expected-error
COPY frw_d FROM STDOUT;

-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed
-- end-expected-error
COPY frw_d FROM STDOUT WITH (FORMAT csv);

-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed
-- end-expected-error
COPY frw_d (i,k) FROM STDOUT;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: COPY commands are only supported using the CopyManager API
-- end-expected-error
COPY frw_d TO STDOUT;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: COPY commands are only supported using the CopyManager API
-- end-expected-error
COPY frw_d TO STDIN;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: COPY commands are only supported using the CopyManager API
-- end-expected-error
COPY frw_d TO STDIN WITH (FORMAT csv);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: COPY commands are only supported using the CopyManager API
-- end-expected-error
COPY (SELECT 1) TO STDIN;

-- A query is a thing to read out of, never one to write into, and that is the
-- grammar's answer rather than the direction word's.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FROM"
-- end-expected-error
COPY (SELECT 1) FROM STDIN;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM frw_d;

DROP TABLE frw_d;
