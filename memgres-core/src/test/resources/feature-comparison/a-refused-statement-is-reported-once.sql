-- ============================================================================
-- A refused statement is reported once and runs once
-- ============================================================================
-- Settling what a statement will answer with is not running it, and a statement
-- that was refused ran no further than the point it was refused at. Every write
-- below is refused for what it names or for the arithmetic it carries, and the
-- relation is left holding exactly the rows it held before: not the row the
-- refused write carried, and not that row twice. The frames these refusals
-- arrive in, and the refusals that are owed to another session, need the raw
-- protocol and a second session and live in the Java test beside this file.
-- ============================================================================

CREATE TABLE rso_t (i int, s text);
INSERT INTO rso_t VALUES (1, 'a');

-- ----------------------------------------------------------------------------
-- 1. A RETURNING list naming a column the relation has not
-- ----------------------------------------------------------------------------

-- outside a block the write is refused for the column it names
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO rso_t VALUES (2, 'b') RETURNING nosuchcol;

BEGIN;

-- inside a block it is refused for the same reason, and not for the block
-- having been aborted by the very statement being reported
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO rso_t VALUES (2, 'b') RETURNING nosuchcol;

ROLLBACK;

BEGIN;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
UPDATE rso_t SET s = 'c' RETURNING nosuchcol;

ROLLBACK;

BEGIN;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
DELETE FROM rso_t WHERE i = 1 RETURNING nosuchcol;

ROLLBACK;

BEGIN;

-- and for a write read through a WITH item
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
WITH d AS (DELETE FROM rso_t WHERE i = 1 RETURNING nosuchcol) SELECT * FROM d;

ROLLBACK;

-- ----------------------------------------------------------------------------
-- 2. A RETURNING expression that cannot be worked out
-- ----------------------------------------------------------------------------

BEGIN;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO rso_t VALUES (3, 'x') RETURNING i / 0;

ROLLBACK;

-- the arithmetic is what the write is refused for outside a block as well
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO rso_t VALUES (3, 'x') RETURNING i / 0;

-- ----------------------------------------------------------------------------
-- 3. A refused write leaves the relation as it was, once
-- ----------------------------------------------------------------------------

-- none of the writes above reached the relation, and none of them reached it
-- twice: one row, the one this file put there
-- begin-expected
-- columns: i | s
-- row: 1, a
-- end-expected
SELECT i, s FROM rso_t ORDER BY i;

-- ----------------------------------------------------------------------------
-- 4. A statement refused inside a block leaves the block with nothing in it
-- ----------------------------------------------------------------------------

BEGIN;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 1 / 0;

ROLLBACK;

-- and the block that was rolled back left the relation alone
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM rso_t;

-- ----------------------------------------------------------------------------
-- 5. A read is refused where it stands, rather than answered with no rows
-- ----------------------------------------------------------------------------

-- a row this query reaches makes the arithmetic impossible
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT i / (i - 1) FROM rso_t;

-- and a value that will not read as the type it is cast to
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer
-- end-expected-error
SELECT s::int FROM rso_t;

-- ----------------------------------------------------------------------------
-- 6. A write that is refused for a row it reaches partway through writes none
--    of the rows it reached before it
-- ----------------------------------------------------------------------------

INSERT INTO rso_t VALUES (2, 'b'), (3, 'c');

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE rso_t SET i = i / (i - 3);

-- begin-expected
-- columns: i | s
-- row: 1, a
-- row: 2, b
-- row: 3, c
-- end-expected
SELECT i, s FROM rso_t ORDER BY i;

DROP TABLE rso_t;
