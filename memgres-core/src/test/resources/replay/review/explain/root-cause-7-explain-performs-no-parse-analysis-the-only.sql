-- source: review-2026-08.md
-- finding: Root cause 7: EXPLAIN performs no parse analysis — the only check is an ad-hoc existence test on top-level FROM items
-- area: EXPLAIN
-- title: Root cause 7: EXPLAIN performs no parse analysis — the only check is an ad-hoc existence test on top-level FROM items
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_absent" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT * FROM zz_absent) s;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_absent" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) INSERT INTO zz_absent VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_absent" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) UPDATE zz_absent SET v = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_absent" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) DELETE FROM zz_absent;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_e WHERE id IN (SELECT id FROM zz_absent);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_absent" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) WITH c AS (SELECT * FROM zz_absent) SELECT * FROM c;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT nosuchcol FROM zz_e;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT nosuchfunc(1);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "nosuchtype" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT 1::nosuchtype;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT 1/0;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT 'abc'::int;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT id, count(*) FROM zz_e;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_e ORDER BY 99;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_e a WHERE b.id = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_e, zz_e;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE MATERIALIZED VIEW zz_mv AS SELECT id FROM zz_t;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_mv" does not exist
-- end-expected-error
SELECT count(*) FROM zz_mv;
-- works
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_mv" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_mv;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Limit
-- row:   ->  Seq Scan on pg_class
-- rowcount: 2
-- end-expected
EXPLAIN (COSTS OFF) SELECT relname FROM pg_class LIMIT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Aggregate
-- row:   ->  Hash Left Join
-- row:         Hash Cond: (c.reloftype = t.oid)
-- row:         ->  Hash Join
-- row:               Hash Cond: (c.relnamespace = nc.oid)
-- row:               ->  Seq Scan on pg_class c
-- row:                     Filter: ((relkind = ANY ('{r,v,f,p}'::"char"[])) AND (pg_has_role(relowner, 'USAGE'::text) OR has_table_privilege(oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'::text) OR has_any_column_privilege(oid, 'SELECT, INSERT, UPDATE, REFERENCES'::text)))
-- row:               ->  Hash
-- row:                     ->  Seq Scan on pg_namespace nc
-- row:                           Filter: (NOT pg_is_other_temp_schema(oid))
-- row:         ->  Hash
-- row:               ->  Hash Join
-- row:                     Hash Cond: (t.typnamespace = nt.oid)
-- row:                     ->  Seq Scan on pg_type t
-- row:                     ->  Hash
-- row:                           ->  Seq Scan on pg_namespace nt
-- rowcount: 16
-- end-expected
EXPLAIN (COSTS OFF) SELECT count(*) FROM information_schema.tables;
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_q (int) AS SELECT $1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: wrong number of parameters for prepared statement "zz_q"
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_q;
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_absent_prep" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_absent_prep;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_q();
-- simple query protocol
-- begin-expected-error
-- sqlstate: 42P02
-- message-like: there is no parameter $1
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT $1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_g" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_g WHERE id = $1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_g" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, GENERIC_PLAN) SELECT * FROM zz_g WHERE id = $0;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_g" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, GENERIC_PLAN) SELECT * FROM zz_g WHERE id = $1;
