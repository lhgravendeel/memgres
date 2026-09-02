-- source: investigation-2026-08.md
-- finding: 359
-- title: tid, xid and cid are not types — they are text and integers wearing a name, and three places disagree about which. RowContext.resolveColumnDef (:533-542) declar
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0)'::tid < '(0,1)'::tid;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(1,2)'::tid > '(0,9)'::tid;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sx (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_sx VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: xid > integer
-- end-expected-error
SELECT count(*) FROM zz_vf2_sx WHERE xmin > 0;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type xid to bigint
-- end-expected-error
SELECT xmin::bigint FROM zz_vf2_sx LIMIT 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(xid) does not exist
-- end-expected-error
SELECT max(xmin) FROM zz_vf2_sx;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an ordering operator for type xid
-- end-expected-error
SELECT id FROM zz_vf2_sx ORDER BY xmin;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: oid + integer
-- end-expected-error
SELECT tableoid + 1 > 0 FROM zz_vf2_sx LIMIT 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function length(tid) does not exist
-- end-expected-error
SELECT length(ctid) FROM zz_vf2_sx LIMIT 1;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type tid: "nope"
-- end-expected-error
SELECT 'nope'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type tid: "(0,70000)"
-- end-expected-error
SELECT '(0,70000)'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type tid: "(0,-1)"
-- end-expected-error
SELECT '(0,-1)'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type tid: "(4294967296,1)"
-- end-expected-error
SELECT '(4294967296,1)'::tid;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type cid: "abc"
-- end-expected-error
SELECT 'abc'::cid;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: xid < xid
-- end-expected-error
SELECT '100'::xid < '200'::xid;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: cid < cid
-- end-expected-error
SELECT '100'::cid < '200'::cid;
