-- ============================================================================
-- What may stand in a partition key
--
-- A partition key has to be settled once, when the row arrives, and read back
-- the same way ever after. A generated column cannot serve: its value is
-- worked out from the rest of the row after the row has already been routed,
-- so PostgreSQL refuses it outright with 42P17 and names the column in the
-- DETAIL. The same code covers a system column and a function that is not
-- IMMUTABLE; an aggregate is 42803 and a window function 42P20.
--
-- Where more than one of those faults stands in the same key list, which one
-- is reported follows the order PostgreSQL reads them in: the whole key list
-- is turned into expressions first, so an aggregate or a window function
-- anywhere in the list is reported before the per-column tests run, and only
-- then are the elements walked left to right for a system or generated column.
--
-- A generated column that is not part of the key is no trouble at all: the
-- partition holds it, computes it and hands it back. So are an identity
-- column and a serial, neither of which is a generated column.
--
-- Every value here was read off PostgreSQL 18.
-- ============================================================================

DROP TABLE IF EXISTS zzj1jt_pk CASCADE;

-- ----------------------------------------------------------------------------
-- A generated column may not stand in a partition key, whatever the strategy
-- ----------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY RANGE (k);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY LIST (k);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY HASH (k);

-- A virtual generated column is refused for the same reason as a stored one.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL) PARTITION BY RANGE (k);

-- ----------------------------------------------------------------------------
-- It may not stand there in company, nor inside an expression
-- ----------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY RANGE (i, k);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY RANGE ((k + 1));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i*2) STORED, m int) PARTITION BY RANGE ((k + m));

-- Where two generated columns stand in one expression, the leftmost is named.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int GENERATED ALWAYS AS (1) STORED, k int GENERATED ALWAYS AS (2) STORED) PARTITION BY RANGE ((k + i));

-- ----------------------------------------------------------------------------
-- Which fault is reported when more than one stands in the key
-- ----------------------------------------------------------------------------

-- The elements are walked left to right, so the generated column comes first.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY RANGE (k, xmin);

-- And with the order turned round, the system column comes first.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use system column "xmin" in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY RANGE (xmin, k);

-- A name that does not exist is settled while the expression is built, which
-- is before any column of the key is looked at.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i * 2) STORED) PARTITION BY RANGE ((k + nosuch));

-- So is an aggregate, wherever in the list it stands.
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i*2) STORED) PARTITION BY RANGE (k, (sum(i)));

-- A function that is not IMMUTABLE is judged with the expression it stands in,
-- again before the generated column two places along is reached.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int GENERATED ALWAYS AS (i*2) STORED) PARTITION BY RANGE ((random()::int), k);

-- ----------------------------------------------------------------------------
-- What else may not stand there
-- ----------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use system column "ctid" in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE (ctid);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use system column "tableoid" in partition key
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE (tableoid);

-- Named inside an expression, a system column is refused under a sentence of
-- its own that does not name the column.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: partition key expressions cannot contain system column references
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE ((xmin::text));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: partition key expressions cannot contain system column references
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE ((ctid::text));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE ((sum(i)));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in partition key expressions
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE ((row_number() OVER ()));

-- Doubled parentheses, so the inner pair is the subquery's own and the key
-- element is read as an expression rather than as a syntax error.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE (((SELECT 1)));

-- A bare name that does not exist is reported as a partition key's own; the
-- same name inside an expression is reported the way any expression's is.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" named in partition key does not exist
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE (nosuch);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k int) PARTITION BY RANGE ((i + nosuch));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE zzj1jt_pk (i int, k timestamptz) PARTITION BY RANGE ((k::date), xmin);

-- Nothing above created anything.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_class WHERE relname = 'zzj1jt_pk';

-- ----------------------------------------------------------------------------
-- A generated column outside the key is no trouble at all
-- ----------------------------------------------------------------------------
CREATE TABLE zzj1jt_gp (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text) PARTITION BY LIST (s);
CREATE TABLE zzj1jt_gp_a PARTITION OF zzj1jt_gp FOR VALUES IN ('a');
INSERT INTO zzj1jt_gp (i, s) VALUES (3, 'a');

-- begin-expected
-- columns: i | k | s
-- row: 3 | 6 | a
-- end-expected
SELECT i, k, s FROM zzj1jt_gp ORDER BY i;

-- The partition computes it too, under the expression it was handed.
-- begin-expected
-- columns: i | k
-- row: 3 | 6
-- end-expected
SELECT i, k FROM zzj1jt_gp_a ORDER BY i;

-- begin-expected
-- columns: partkeydef
-- row: LIST (s)
-- end-expected
SELECT pg_get_partkeydef('zzj1jt_gp'::regclass) AS partkeydef;

DROP TABLE zzj1jt_gp CASCADE;

-- ----------------------------------------------------------------------------
-- An identity column and a serial are not generated columns, and may stand
-- ----------------------------------------------------------------------------
CREATE TABLE zzj1jt_id (i int GENERATED ALWAYS AS IDENTITY, k int) PARTITION BY RANGE (i);

-- begin-expected
-- columns: partkeydef
-- row: RANGE (i)
-- end-expected
SELECT pg_get_partkeydef('zzj1jt_id'::regclass) AS partkeydef;

DROP TABLE zzj1jt_id CASCADE;

CREATE TABLE zzj1jt_sp (i serial, k int) PARTITION BY RANGE (i);

-- begin-expected
-- columns: partkeydef
-- row: RANGE (i)
-- end-expected
SELECT pg_get_partkeydef('zzj1jt_sp'::regclass) AS partkeydef;

DROP TABLE zzj1jt_sp CASCADE;
