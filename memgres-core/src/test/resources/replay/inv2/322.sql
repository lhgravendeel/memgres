-- source: investigation-2026-08.md
-- finding: 322
-- title: EXPLAIN performs no parse analysis — the only check is an ad-hoc existence test on top-level FROM items via executor.resolveTable, which also does not know matv
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
