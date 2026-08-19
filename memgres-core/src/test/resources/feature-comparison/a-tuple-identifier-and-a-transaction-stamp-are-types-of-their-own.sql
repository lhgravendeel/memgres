-- ============================================================================
-- A tuple identifier and a transaction stamp are types of their own
--
-- The six columns a relation carries without declaring them have types like any other column: the
-- tuple identifier is a tid, the two transaction stamps are xids, the two command stamps are cids,
-- and the relation stamp is an oid. Nothing chose those types, which is exactly why they are not
-- negotiable -- a value of one prints as text but is not text, and every rule that decides
-- something from a type decides it from these.
--
-- A tid is a block and a slot within it, ordered by the block and then by the slot. An xid and a
-- cid have equality and nothing else: PostgreSQL registers no ordering over either, so nothing is
-- sorted, partitioned or ranked by one. No cast turns any of the three into a number, and no
-- function is declared over them beyond the ones that take anything at all.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE tidt_a (id int, v text);
INSERT INTO tidt_a VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- ============================================================================
-- A tuple identifier is a block and a slot within it
-- ============================================================================
-- begin-expected
-- columns: text
-- row: (0,1)
-- end-expected
SELECT '(0,1)'::tid::text;
-- begin-expected
-- columns: pg_typeof
-- row: tid
-- end-expected
SELECT pg_typeof('(0,1)'::tid)::text;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '(0,1)'::tid = '(0,1)'::tid;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '(0,1)'::tid <> '(0,2)'::tid;
-- begin-expected
-- columns: text
-- row: (4294967295,65535)
-- end-expected
SELECT '(4294967295,65535)'::tid::text;

-- The slot is a number, not the digits it was written with.
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '(0,10)'::tid > '(0,9)'::tid;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '(0,9)'::tid < '(0,10)'::tid;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '(1,1)'::tid > '(0,9)'::tid;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '(0,1)'::tid <= '(0,1)'::tid;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '(0,1)'::tid >= '(0,1)'::tid;

-- ============================================================================
-- What is not a block and a slot is not a tuple identifier
-- ============================================================================
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 'nope'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT ''::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '(0,1'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '0,1'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '()'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '(0)'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '(0,70000)'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '(0,-1)'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '(4294967296,1)'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '( 0 , 1 )'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '(-2147483649,1)'::tid;

-- The block is a signed number read as the unsigned one it is, and what follows the paren is
-- not read at all.
-- begin-expected
-- columns: text
-- row: (4294967295,1)
-- end-expected
SELECT '(-1,1)'::tid::text;
-- begin-expected
-- columns: text
-- row: (2147483648,65535)
-- end-expected
SELECT '(-2147483648,65535)'::tid::text;
-- begin-expected
-- columns: text
-- row: (0,1)
-- end-expected
SELECT '(0,1)x'::tid::text;
-- begin-expected
-- columns: text
-- row: (0,1)
-- end-expected
SELECT ' (0,1) '::tid::text;
-- begin-expected
-- columns: text
-- row: (1,1)
-- end-expected
SELECT '(+1,001)'::tid::text;

-- ============================================================================
-- A command counter counts the commands of one transaction
-- ============================================================================
-- begin-expected
-- columns: text
-- row: 0
-- end-expected
SELECT '0'::cid::text;
-- begin-expected
-- columns: text
-- row: 100
-- end-expected
SELECT '100'::cid::text;
-- begin-expected
-- columns: text
-- row: 4294967295
-- end-expected
SELECT '4294967295'::cid::text;
-- begin-expected
-- columns: pg_typeof
-- row: cid
-- end-expected
SELECT pg_typeof('100'::cid)::text;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '100'::cid = '100'::cid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT 'abc'::cid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT ''::cid;
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected-error
SELECT '1.5'::cid;

-- ============================================================================
-- A stamp has equality and nothing else
-- ============================================================================
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '5'::xid = '5'::xid;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '5'::xid <> '6'::xid;
-- begin-expected
-- columns: pg_typeof
-- row: xid
-- end-expected
SELECT pg_typeof('5'::xid)::text;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '5'::xid > '4'::xid;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '5'::xid < '6'::xid;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '5'::xid >= '4'::xid;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '5'::cid > '4'::cid;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT '5'::cid < '6'::cid;

-- ============================================================================
-- The columns nobody declared carry those types
-- ============================================================================
-- begin-expected
-- columns: pg_typeof
-- row: tid
-- row: tid
-- row: tid
-- end-expected
SELECT pg_typeof(ctid)::text FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: pg_typeof
-- row: xid
-- row: xid
-- row: xid
-- end-expected
SELECT pg_typeof(xmin)::text FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: pg_typeof
-- row: xid
-- row: xid
-- row: xid
-- end-expected
SELECT pg_typeof(xmax)::text FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: pg_typeof
-- row: cid
-- row: cid
-- row: cid
-- end-expected
SELECT pg_typeof(cmin)::text FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: pg_typeof
-- row: cid
-- row: cid
-- row: cid
-- end-expected
SELECT pg_typeof(cmax)::text FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: pg_typeof
-- row: oid
-- row: oid
-- row: oid
-- end-expected
SELECT pg_typeof(tableoid)::text FROM tidt_a ORDER BY id;

-- ============================================================================
-- An operator the type does not have is an operator that does not exist
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT xmin > 0 FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT xmin < 100 FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT xmax >= 0 FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT cmin > 0 FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT cmax < 1 FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT ctid > 0 FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT tableoid + 1 FROM tidt_a;

-- The ones it does have work.
-- begin-expected
-- columns: ?column?
-- row: f
-- row: f
-- row: f
-- end-expected
SELECT xmin = 0 FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: ?column?
-- row: t
-- row: f
-- row: f
-- end-expected
SELECT ctid = '(0,1)'::tid FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: ?column?
-- row: t
-- row: t
-- row: t
-- end-expected
SELECT tableoid > 0 FROM tidt_a ORDER BY id;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT ctid::text FROM tidt_a ORDER BY id;

-- ============================================================================
-- No cast turns one of them into a number
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT ctid::bigint FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT ctid::int FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT ctid::numeric FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT xmin::bigint FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT xmin::int FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT cmin::int FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42846
-- end-expected-error
SELECT cmax::numeric FROM tidt_a;

-- ============================================================================
-- Nothing is sorted by a stamp
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id FROM tidt_a ORDER BY xmin;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id FROM tidt_a ORDER BY xmax;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id FROM tidt_a ORDER BY cmin;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT id FROM tidt_a ORDER BY cmax DESC;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT row_number() OVER (ORDER BY xmin) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
SELECT row_number() OVER (PARTITION BY cmin) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT array_agg(id ORDER BY xmin) FROM tidt_a;

-- A tid and an oid are ordered, so they sort.
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM tidt_a ORDER BY ctid;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM tidt_a ORDER BY tableoid, id;
-- begin-expected
-- columns: array_agg
-- row: {1,2,3}
-- end-expected
SELECT array_agg(id ORDER BY ctid) FROM tidt_a;
-- begin-expected
-- columns: id
-- row: 3
-- row: 2
-- row: 1
-- end-expected
SELECT id FROM tidt_a ORDER BY ctid DESC;

-- ============================================================================
-- No function is declared over them
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT length(ctid) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT length(xmin) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT upper(xmin) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT abs(cmin) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT max(xmin) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT min(cmin) FROM tidt_a;
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT sum(cmin) FROM tidt_a;

-- The ones declared over anything at all, and the ones declared over tid and oid, do exist.
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(ctid) FROM tidt_a;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(xmin) FROM tidt_a;
-- begin-expected
-- columns: max
-- row: (0,3)
-- end-expected
SELECT max(ctid)::text FROM tidt_a;
-- begin-expected
-- columns: min
-- row: (0,1)
-- end-expected
SELECT min(ctid)::text FROM tidt_a;
-- begin-expected
-- columns: pg_typeof
-- row: tid
-- end-expected
SELECT pg_typeof(max(ctid))::text FROM tidt_a;

-- ============================================================================
-- Equality is enough to group and to make distinct
-- ============================================================================
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT DISTINCT xmin FROM tidt_a) s;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM (SELECT DISTINCT ctid FROM tidt_a) s;
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT xmin FROM tidt_a GROUP BY xmin) s;
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT cmin FROM tidt_a GROUP BY cmin) s;

-- teardown
DROP TABLE tidt_a;
