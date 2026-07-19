-- ============================================================================
-- Feature Comparison: ALTER TABLE constraint integrity
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers:
--   1. RENAME COLUMN keeps PK/UNIQUE/CHECK/FK enforcement attached
--   2. ADD PRIMARY KEY / ADD UNIQUE validate existing rows (23505/23502)
--      and ADD PRIMARY KEY marks columns NOT NULL
--   3. ADD COLUMN volatile default evaluates per row; serial/identity backfill
--   4. ALTER COLUMN TYPE without USING only allows assignment casts (42804)
--   5. DROP COLUMN drops dependent constraints
-- ============================================================================

-- ============================================================================
-- 1. RENAME COLUMN keeps constraints attached
-- ============================================================================

-- setup
CREATE TABLE atci_pk (id int PRIMARY KEY, v text);
INSERT INTO atci_pk VALUES (1, 'a');
ALTER TABLE atci_pk RENAME COLUMN id TO ident;

-- stmt: duplicate PK still rejected under the new column name
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO atci_pk VALUES (1, 'b');

-- stmt: non-duplicate insert still works
INSERT INTO atci_pk VALUES (2, 'b');

-- stmt: old column name is gone
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
INSERT INTO atci_pk (id, v) VALUES (3, 'c');

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM atci_pk;

-- setup: CHECK constraint survives a rename of its column
CREATE TABLE atci_ck (id int, price int CHECK (price > 0));
INSERT INTO atci_ck VALUES (1, 10);
ALTER TABLE atci_ck RENAME COLUMN price TO amount;

-- stmt: valid insert must not fail with 42703 (stale old name inside CHECK)
INSERT INTO atci_ck VALUES (2, 20);

-- stmt: CHECK still enforced under the new name
-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO atci_ck VALUES (3, -5);

-- setup: FK on another table follows the referenced column rename
CREATE TABLE atci_parent (id int PRIMARY KEY);
CREATE TABLE atci_child (cid int, pid int REFERENCES atci_parent(id));
INSERT INTO atci_parent VALUES (1);
ALTER TABLE atci_parent RENAME COLUMN id TO pkid;

-- stmt: FK still enforced after referenced-column rename
-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO atci_child VALUES (1, 99);

-- stmt: valid FK value still accepted
INSERT INTO atci_child VALUES (2, 1);

-- stmt: constraint catalog reflects the new column name
-- begin-expected
-- columns: column_name
-- row: ident
-- end-expected
SELECT column_name FROM information_schema.constraint_column_usage
WHERE constraint_name = 'atci_pk_pkey';

-- ============================================================================
-- 2. ADD PRIMARY KEY / ADD UNIQUE on existing data
-- ============================================================================

-- setup
CREATE TABLE atci_dup (id int, v text);
INSERT INTO atci_dup VALUES (1, 'a'), (1, 'b');

-- stmt: ADD PRIMARY KEY over duplicate data must fail
-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index
-- end-expected-error
ALTER TABLE atci_dup ADD PRIMARY KEY (id);

-- stmt: the failed constraint was not half-installed
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint
WHERE conrelid = 'atci_dup'::regclass AND contype = 'p';

-- setup
CREATE TABLE atci_nulls (id int, v text);
INSERT INTO atci_nulls VALUES (1, 'a'), (NULL, 'b');

-- stmt: ADD PRIMARY KEY over NULLs must fail
-- begin-expected-error
-- sqlstate: 23502
-- message-like: contains null values
-- end-expected-error
ALTER TABLE atci_nulls ADD PRIMARY KEY (id);

-- setup
CREATE TABLE atci_addpk (id int, v text);
INSERT INTO atci_addpk VALUES (1, 'a'), (2, 'b');
ALTER TABLE atci_addpk ADD PRIMARY KEY (id);

-- stmt: successful ADD PRIMARY KEY marks the column NOT NULL
-- begin-expected
-- columns: is_nullable
-- row: NO
-- end-expected
SELECT is_nullable FROM information_schema.columns
WHERE table_name = 'atci_addpk' AND column_name = 'id';

-- begin-expected
-- columns: attnotnull
-- row: true
-- end-expected
SELECT attnotnull FROM pg_attribute
WHERE attrelid = 'atci_addpk'::regclass AND attname = 'id';

-- stmt: NOT NULL is actually enforced afterwards
-- begin-expected-error
-- sqlstate: 23502
-- end-expected-error
INSERT INTO atci_addpk VALUES (NULL, 'c');

-- setup
CREATE TABLE atci_uq (id int, email text);
INSERT INTO atci_uq VALUES (1, 'x@y.com'), (2, 'x@y.com');

-- stmt: ADD UNIQUE over duplicate data must fail
-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index
-- end-expected-error
ALTER TABLE atci_uq ADD CONSTRAINT atci_uq_email_key UNIQUE (email);

-- setup: NULLs are distinct by default, so multiple NULLs are fine
CREATE TABLE atci_uqnull (id int, email text);
INSERT INTO atci_uqnull VALUES (1, NULL), (2, NULL);
ALTER TABLE atci_uqnull ADD CONSTRAINT atci_uqnull_email_key UNIQUE (email);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_constraint WHERE conname = 'atci_uqnull_email_key';

-- ============================================================================
-- 3. ADD COLUMN volatile default / serial backfill
-- ============================================================================

-- setup
CREATE TABLE atci_uuid (id int);
INSERT INTO atci_uuid VALUES (1), (2), (3), (4), (5);
ALTER TABLE atci_uuid ADD COLUMN u uuid DEFAULT gen_random_uuid();

-- stmt: volatile default -> a distinct value per existing row, none NULL
-- begin-expected
-- columns: all_distinct
-- row: true
-- end-expected
SELECT count(DISTINCT u) = count(*) AS all_distinct FROM atci_uuid;

-- begin-expected
-- columns: n_null
-- row: 0
-- end-expected
SELECT count(*) AS n_null FROM atci_uuid WHERE u IS NULL;

-- setup
CREATE TABLE atci_rand (id int);
INSERT INTO atci_rand SELECT g FROM generate_series(1, 20) g;
ALTER TABLE atci_rand ADD COLUMN r double precision DEFAULT random();

-- begin-expected
-- columns: all_distinct
-- row: true
-- end-expected
SELECT count(DISTINCT r) = count(*) AS all_distinct FROM atci_rand;

-- setup: now() is STABLE -> one value per statement is correct
CREATE TABLE atci_now (id int);
INSERT INTO atci_now VALUES (1), (2), (3);
ALTER TABLE atci_now ADD COLUMN ts timestamptz DEFAULT now();

-- begin-expected
-- columns: n_distinct
-- row: 1
-- end-expected
SELECT count(DISTINCT ts) AS n_distinct FROM atci_now;

-- setup: ADD COLUMN serial backfills existing rows and is NOT NULL
CREATE TABLE atci_serial (v text);
INSERT INTO atci_serial VALUES ('a'), ('b'), ('c');
ALTER TABLE atci_serial ADD COLUMN id serial;

-- begin-expected
-- columns: n_null | n_distinct | min_id | max_id
-- row: 0, 3, 1, 3
-- end-expected
SELECT count(*) FILTER (WHERE id IS NULL) AS n_null,
       count(DISTINCT id) AS n_distinct,
       min(id) AS min_id,
       max(id) AS max_id
FROM atci_serial;

-- begin-expected
-- columns: is_nullable
-- row: NO
-- end-expected
SELECT is_nullable FROM information_schema.columns
WHERE table_name = 'atci_serial' AND column_name = 'id';

-- stmt: subsequent inserts continue the sequence
INSERT INTO atci_serial (v) VALUES ('d');

-- begin-expected
-- columns: id
-- row: 4
-- end-expected
SELECT id FROM atci_serial WHERE v = 'd';

-- setup: identity column backfills too
CREATE TABLE atci_ident (v text);
INSERT INTO atci_ident VALUES ('a'), ('b');
ALTER TABLE atci_ident ADD COLUMN id int GENERATED ALWAYS AS IDENTITY;

-- begin-expected
-- columns: n_null | n_distinct
-- row: 0, 2
-- end-expected
SELECT count(*) FILTER (WHERE id IS NULL) AS n_null,
       count(DISTINCT id) AS n_distinct
FROM atci_ident;

-- ============================================================================
-- 4. ALTER COLUMN TYPE without USING
-- ============================================================================

-- setup
CREATE TABLE atci_cast (t text, vc varchar(10), i int, b bigint);
INSERT INTO atci_cast VALUES ('123', 'hello', 42, 7);

-- stmt: text -> integer without USING must fail
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot be cast automatically
-- end-expected-error
ALTER TABLE atci_cast ALTER COLUMN t TYPE integer;

-- stmt: text -> date without USING must fail
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot be cast automatically
-- end-expected-error
ALTER TABLE atci_cast ALTER COLUMN t TYPE date;

-- stmt: int -> boolean without USING must fail
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot be cast automatically
-- end-expected-error
ALTER TABLE atci_cast ALTER COLUMN i TYPE boolean;

-- stmt: varchar -> text without USING works (same category)
ALTER TABLE atci_cast ALTER COLUMN vc TYPE text;

-- stmt: int -> bigint without USING works (numeric widening)
ALTER TABLE atci_cast ALTER COLUMN i TYPE bigint;

-- stmt: int -> text without USING works (assignment cast to string category)
ALTER TABLE atci_cast ALTER COLUMN b TYPE text;

-- stmt: text -> integer WITH an explicit USING works
ALTER TABLE atci_cast ALTER COLUMN t TYPE integer USING t::integer;

-- begin-expected
-- columns: t | vc | i | b
-- row: 124, hello, 42, 7
-- end-expected
SELECT t + 1 AS t, vc, i, b FROM atci_cast;

-- ============================================================================
-- 5. DROP COLUMN drops dependent constraints
-- ============================================================================

-- setup
CREATE TABLE atci_dc (a int, b int, c int,
    CONSTRAINT atci_dc_b_key UNIQUE (b),
    CONSTRAINT atci_dc_c_check CHECK (c > 0));
ALTER TABLE atci_dc DROP COLUMN b;

-- stmt: single-column UNIQUE on the dropped column is gone from pg_constraint
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint WHERE conname = 'atci_dc_b_key';

ALTER TABLE atci_dc DROP COLUMN c;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint WHERE conname = 'atci_dc_c_check';

-- setup: multi-column UNIQUE containing the dropped column is dropped entirely
CREATE TABLE atci_dcm (a int, b int, CONSTRAINT atci_dcm_key UNIQUE (a, b));
INSERT INTO atci_dcm VALUES (1, 1), (1, 2);
ALTER TABLE atci_dcm DROP COLUMN b;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint WHERE conname = 'atci_dcm_key';

-- stmt: uniqueness on the surviving column is not enforced (constraint dropped, not shrunk)
INSERT INTO atci_dcm VALUES (1);

-- setup: dropping a column referenced by another table's FK needs CASCADE
CREATE TABLE atci_dcp (id int PRIMARY KEY, v text);
CREATE TABLE atci_dcc (pid int REFERENCES atci_dcp(id));

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: because other objects depend on it
-- end-expected-error
ALTER TABLE atci_dcp DROP COLUMN id;

ALTER TABLE atci_dcp DROP COLUMN id CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint
WHERE conrelid = 'atci_dcc'::regclass AND contype = 'f';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE atci_dcc;
DROP TABLE atci_dcp;
DROP TABLE atci_dcm;
DROP TABLE atci_dc;
DROP TABLE atci_cast;
DROP TABLE atci_ident;
DROP TABLE atci_serial;
DROP TABLE atci_now;
DROP TABLE atci_rand;
DROP TABLE atci_uuid;
DROP TABLE atci_uqnull;
DROP TABLE atci_uq;
DROP TABLE atci_addpk;
DROP TABLE atci_nulls;
DROP TABLE atci_dup;
DROP TABLE atci_child;
DROP TABLE atci_parent;
DROP TABLE atci_ck;
DROP TABLE atci_pk;
