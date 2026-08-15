-- ============================================================================
-- In what order a statement's faults are reported
--
-- A statement can be wrong in more than one way at once, and PostgreSQL
-- reports exactly one of them: the first it meets while it reads the
-- statement. The reading has an order of its own, and it is not the order the
-- text spells -- a query's WITH items and FROM items are read before anything
-- it selects, an UPDATE's FROM and WHERE before the values it assigns and the
-- columns it assigns to, an INSERT's column list before its values, and the
-- relations a clause names before that clause's expressions. Within one
-- expression the reading is left to right.
--
-- The two faults paired off below are a column the statement cannot resolve
-- and the DEFAULT keyword written where it may not stand. Written in one order
-- the column is reported; written in the other, the keyword. The character
-- offset each error points at is checked in the client tests, which can read
-- the Position field; here it is the choice of error being pinned down.
--
-- Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE dfo_zz (a text DEFAULT 'D', b int);
CREATE TABLE dfo_uu (b int, c text);

-- ============================================================================
-- The two operands of one operator, in both orders
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT a FROM dfo_zz WHERE nosuchcol = DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT a FROM dfo_zz WHERE DEFAULT = nosuchcol;

-- a call's arguments are read before the call itself is looked up
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT a FROM dfo_zz WHERE upper(nosuchcol) = DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT a FROM dfo_zz WHERE upper(DEFAULT) = nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT concat(nosuchcol, DEFAULT) FROM dfo_zz;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT concat(DEFAULT, nosuchcol) FROM dfo_zz;

-- parentheses reorder nothing
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT a FROM dfo_zz WHERE (b + nosuchcol) = (1 + DEFAULT);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT a FROM dfo_zz WHERE (b + DEFAULT) = (1 + nosuchcol);

-- ============================================================================
-- The clauses of a query, in the order they are read: the select list, then
-- WHERE, then HAVING, then the sort clause, and the grouping items last
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT nosuchcol FROM dfo_zz WHERE DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT DEFAULT FROM dfo_zz WHERE nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT a FROM dfo_zz GROUP BY a HAVING nosuchcol AND DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT a FROM dfo_zz GROUP BY a HAVING DEFAULT AND nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT a FROM dfo_zz WHERE nosuchcol ORDER BY DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT a FROM dfo_zz WHERE DEFAULT ORDER BY nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT nosuchcol FROM dfo_zz GROUP BY DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT DEFAULT FROM dfo_zz GROUP BY nosuchcol;

-- ============================================================================
-- A FROM item and a WITH item are read before the query that reads from them
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT nosuchcol FROM dfo_zz JOIN dfo_uu ON DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT a FROM dfo_zz t1, (SELECT DEFAULT) s WHERE DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
UPDATE dfo_zz SET a = DEFAULT || 'x' FROM (SELECT DEFAULT) s;

-- DISTINCT ON is read after WHERE, however early it is written
-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT DISTINCT ON (DEFAULT) a FROM dfo_zz WHERE DEFAULT;

-- but the relations a clause names are resolved before its expressions
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dfo_qq" does not exist
-- end-expected-error
SELECT a FROM dfo_zz JOIN dfo_qq ON DEFAULT;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dfo_qq" does not exist
-- end-expected-error
WITH w AS (SELECT DEFAULT FROM dfo_qq) SELECT 1;

-- a column of a WITH item is resolved while the item is read, which is before
-- the keyword standing in the query around it
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
WITH w AS (SELECT nosuchcol FROM dfo_zz) SELECT DEFAULT;

-- ============================================================================
-- An UPDATE settles what every assignment writes before it resolves any of the
-- columns written to; an INSERT resolves its column list before its values
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dfo_zz" does not exist
-- end-expected-error
UPDATE dfo_zz SET nosuchcol = DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
UPDATE dfo_zz SET nosuchcol = DEFAULT || 'x';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
UPDATE dfo_zz SET nosuchcol = 1, a = DEFAULT || 'x';

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dfo_zz" does not exist
-- end-expected-error
INSERT INTO dfo_zz (nosuchcol) VALUES (DEFAULT || 'x');

-- ============================================================================
-- A statement holding more than one DEFAULT is refused for the one that was
-- refused, and the legal ones do not count
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
INSERT INTO dfo_zz (a,b) VALUES (DEFAULT,1) RETURNING DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
INSERT INTO dfo_zz (a,b) VALUES (DEFAULT, DEFAULT) RETURNING DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
UPDATE dfo_zz SET a = DEFAULT RETURNING DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
UPDATE dfo_zz SET a = DEFAULT, b = DEFAULT + 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
UPDATE dfo_zz SET a = DEFAULT || 'x', b = DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
INSERT INTO dfo_zz (a,b) VALUES (DEFAULT || 'x', 1) RETURNING DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
UPDATE dfo_zz SET a = DEFAULT || 'x' WHERE b = DEFAULT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: DEFAULT is not allowed in this context
-- end-expected-error
SELECT DEFAULT FROM (SELECT DEFAULT) x;

-- nothing above wrote a row
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM dfo_zz;

-- and the legal placements are untouched
INSERT INTO dfo_zz (a,b) VALUES (DEFAULT, 1);
UPDATE dfo_zz SET a = DEFAULT, b = 2;

-- begin-expected
-- columns: a | b
-- row: D | 2
-- end-expected
SELECT a, b FROM dfo_zz;

-- cleanup
DROP TABLE dfo_uu;
DROP TABLE dfo_zz;
