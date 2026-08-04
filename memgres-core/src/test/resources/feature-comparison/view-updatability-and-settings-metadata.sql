-- Whether a view accepts a write, and whether the catalog says the same thing the write does.
--
-- A view containing a set-returning function was written through, so a caller changed rows
-- PostgreSQL would never have let it touch. And for six other shapes the catalog and the
-- executor disagreed: information_schema.views.is_updatable said YES for views the engine
-- refused, information_schema.columns.is_updatable was the constant YES, and the two functions
-- a client calls to check did not exist.
--
-- Every case below names a specific relation and a specific column. None of them counts rows in
-- a catalog: the reference server carries extensions and leftovers memgres does not, so a count
-- would differ for reasons that are not memgres's.

DROP VIEW IF EXISTS vum_nested, vum_srf, vum_union, vum_agg, vum_window, vum_with,
                    vum_offset, vum_limit, vum_having, vum_group, vum_distinct,
                    vum_const, vum_expr, vum_star, vum_order, vum_where, vum_simple CASCADE;
DROP TABLE IF EXISTS vum_base CASCADE;

CREATE TABLE vum_base (id int PRIMARY KEY, val text, n int);
INSERT INTO vum_base VALUES (1,'a',1),(2,'b',2),(3,'c',3);

CREATE VIEW vum_simple AS SELECT id, val, n FROM vum_base;
CREATE VIEW vum_where AS SELECT id, val, n FROM vum_base WHERE n > 0;
CREATE VIEW vum_order AS SELECT id, val, n FROM vum_base ORDER BY id;
CREATE VIEW vum_star AS SELECT * FROM vum_base;
CREATE VIEW vum_expr AS SELECT id, val, n * 2 AS n2 FROM vum_base;
CREATE VIEW vum_const AS SELECT id, val, 42 AS k FROM vum_base;
CREATE VIEW vum_distinct AS SELECT DISTINCT id, val, n FROM vum_base;
CREATE VIEW vum_group AS SELECT id, count(*) AS c FROM vum_base GROUP BY id;
CREATE VIEW vum_having AS SELECT id, count(*) AS c FROM vum_base GROUP BY id HAVING count(*) > 0;
CREATE VIEW vum_limit AS SELECT id, val, n FROM vum_base LIMIT 5;
CREATE VIEW vum_offset AS SELECT id, val, n FROM vum_base OFFSET 1;
CREATE VIEW vum_with AS WITH x AS (SELECT id, val, n FROM vum_base) SELECT id, val, n FROM x;
CREATE VIEW vum_window AS SELECT id, val, n, row_number() OVER () AS rn FROM vum_base;
CREATE VIEW vum_agg AS SELECT count(*) AS c FROM vum_base;
CREATE VIEW vum_srf AS SELECT id, val, generate_series(1,2) AS g FROM vum_base;
CREATE VIEW vum_union AS SELECT id, val, n FROM vum_base UNION ALL SELECT id, val, n FROM vum_base;
CREATE VIEW vum_nested AS SELECT id, val, n FROM vum_distinct;

-- ---------------------------------------------------------------------------
-- The write itself
-- ---------------------------------------------------------------------------

-- The recorded failure: a set-returning function in the select list.
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_srf"
-- end-expected-error
INSERT INTO vum_srf (id, val) VALUES (107,'ins');

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot update view "vum_srf"
-- end-expected-error
UPDATE vum_srf SET val = 'upd' WHERE id = 1;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view "vum_srf"
-- end-expected-error
DELETE FROM vum_srf WHERE id = 1;

-- The base table was not touched by any of the three.
-- begin-expected
-- columns: id,val,n
-- row: 1|a|1
-- row: 2|b|2
-- row: 3|c|3
-- end-expected
SELECT id, val, n FROM vum_base ORDER BY id;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view "vum_distinct"
-- end-expected-error
DELETE FROM vum_distinct WHERE id = 1;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_group"
-- end-expected-error
INSERT INTO vum_group (id) VALUES (9);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_limit"
-- end-expected-error
INSERT INTO vum_limit (id, val, n) VALUES (9,'x',1);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_offset"
-- end-expected-error
INSERT INTO vum_offset (id, val, n) VALUES (9,'x',1);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_with"
-- end-expected-error
INSERT INTO vum_with (id, val, n) VALUES (9,'x',1);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_window"
-- end-expected-error
INSERT INTO vum_window (id, val, n) VALUES (9,'x',1);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_agg"
-- end-expected-error
INSERT INTO vum_agg (c) VALUES (1);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_union"
-- end-expected-error
INSERT INTO vum_union (id, val, n) VALUES (9,'x',1);

-- A view over a non-updatable view: PostgreSQL names the inner view, not the one written to.
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "vum_distinct"
-- end-expected-error
INSERT INTO vum_nested (id, val, n) VALUES (9,'x',1);

-- A column that is not a column of the base relation: the relation takes the write, that one
-- column does not, and the class says so rather than claiming the column is missing.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot insert into column "n2" of view "vum_expr"
-- end-expected-error
INSERT INTO vum_expr (id, val, n2) VALUES (108,'ins',4);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot update column "n2" of view "vum_expr"
-- end-expected-error
UPDATE vum_expr SET n2 = 4 WHERE id = 1;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot insert into column "k" of view "vum_const"
-- end-expected-error
INSERT INTO vum_const (id, val, k) VALUES (109,'ins',1);

-- The converse: an auto-updatable view still takes the write.
INSERT INTO vum_simple (id, val, n) VALUES (50,'ins',5);
UPDATE vum_where SET val = 'w' WHERE id = 50;
UPDATE vum_order SET n = 6 WHERE id = 50;
UPDATE vum_star SET n = 7 WHERE id = 50;
UPDATE vum_expr SET val = 'e' WHERE id = 50;
UPDATE vum_const SET val = 'k' WHERE id = 50;

-- begin-expected
-- columns: id,val,n
-- row: 50|k|7
-- end-expected
SELECT id, val, n FROM vum_base WHERE id = 50;

DELETE FROM vum_simple WHERE id = 50;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM vum_base WHERE id = 50;

-- ---------------------------------------------------------------------------
-- The catalog agreeing with it
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: table_name,is_updatable,is_insertable_into
-- row: vum_agg|NO|NO
-- row: vum_const|YES|YES
-- row: vum_distinct|NO|NO
-- row: vum_expr|YES|YES
-- row: vum_group|NO|NO
-- row: vum_having|NO|NO
-- row: vum_limit|NO|NO
-- row: vum_nested|NO|NO
-- row: vum_offset|NO|NO
-- row: vum_order|YES|YES
-- row: vum_simple|YES|YES
-- row: vum_srf|NO|NO
-- row: vum_star|YES|YES
-- row: vum_union|NO|NO
-- row: vum_where|YES|YES
-- row: vum_window|NO|NO
-- row: vum_with|NO|NO
-- end-expected
SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views WHERE table_name LIKE 'vum\_%' ORDER BY table_name;

-- begin-expected
-- columns: table_name,is_insertable_into
-- row: vum_agg|NO
-- row: vum_base|YES
-- row: vum_distinct|NO
-- row: vum_simple|YES
-- end-expected
SELECT table_name, is_insertable_into FROM information_schema.tables
 WHERE table_name IN ('vum_base','vum_simple','vum_agg','vum_distinct') ORDER BY table_name;

-- Column-level updatability. The constant YES was hiding every one of these.
-- begin-expected
-- columns: table_name,column_name,is_updatable
-- row: vum_agg|c|NO
-- row: vum_base|id|YES
-- row: vum_const|k|NO
-- row: vum_distinct|id|NO
-- row: vum_expr|id|YES
-- row: vum_expr|n2|NO
-- row: vum_nested|id|NO
-- row: vum_union|id|NO
-- row: vum_window|rn|NO
-- end-expected
SELECT table_name, column_name, is_updatable FROM information_schema.columns
 WHERE (table_name, column_name) IN (('vum_base','id'), ('vum_expr','id'), ('vum_expr','n2'),
                                     ('vum_const','k'), ('vum_distinct','id'), ('vum_agg','c'),
                                     ('vum_window','rn'), ('vum_union','id'), ('vum_nested','id'))
 ORDER BY table_name, column_name;

-- pg_relation_is_updatable packs the events into a bitmask: 4 UPDATE, 8 INSERT, 16 DELETE.
-- begin-expected
-- columns: u
-- row: 28
-- end-expected
SELECT pg_relation_is_updatable('vum_base'::regclass, false) AS u;

-- begin-expected
-- columns: u
-- row: 28
-- end-expected
SELECT pg_relation_is_updatable('vum_simple'::regclass, false) AS u;

-- begin-expected
-- columns: u
-- row: 28
-- end-expected
SELECT pg_relation_is_updatable('vum_expr'::regclass, false) AS u;

-- begin-expected
-- columns: u
-- row: 0
-- end-expected
SELECT pg_relation_is_updatable('vum_distinct'::regclass, false) AS u;

-- begin-expected
-- columns: u
-- row: 0
-- end-expected
SELECT pg_relation_is_updatable('vum_with'::regclass, false) AS u;

-- begin-expected
-- columns: u
-- row: 0
-- end-expected
SELECT pg_relation_is_updatable('vum_srf'::regclass, false) AS u;

-- begin-expected
-- columns: u
-- row: 0
-- end-expected
SELECT pg_relation_is_updatable('vum_nested'::regclass, false) AS u;

-- An oid that reaches no relation answers 0 rather than raising.
-- begin-expected
-- columns: u
-- row: 0
-- end-expected
SELECT pg_relation_is_updatable(0::oid, false) AS u;

-- begin-expected
-- columns: c1,c2,c3
-- row: t|t|f
-- end-expected
SELECT pg_column_is_updatable('vum_expr'::regclass, 1::smallint, false) AS c1,
       pg_column_is_updatable('vum_expr'::regclass, 2::smallint, false) AS c2,
       pg_column_is_updatable('vum_expr'::regclass, 3::smallint, false) AS c3;

-- begin-expected
-- columns: c1,c2
-- row: f|f
-- end-expected
SELECT pg_column_is_updatable('vum_distinct'::regclass, 1::smallint, false) AS c1,
       pg_column_is_updatable(0::oid, 1::smallint, false) AS c2;

-- Both functions are declared where a client looks for them.
-- begin-expected
-- columns: proname,args,rettype
-- row: pg_column_is_updatable|regclass, smallint, boolean|bool
-- row: pg_relation_is_updatable|regclass, boolean|int4
-- end-expected
SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args, t.typname AS rettype
  FROM pg_proc p JOIN pg_type t ON t.oid = p.prorettype
 WHERE p.proname IN ('pg_relation_is_updatable','pg_column_is_updatable')
 ORDER BY p.proname;

-- ---------------------------------------------------------------------------
-- An INSTEAD OF trigger takes the write its own event names, and no other
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS vum_trigview CASCADE;
DROP FUNCTION IF EXISTS vum_trg() CASCADE;
CREATE FUNCTION vum_trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE VIEW vum_trigview AS SELECT DISTINCT id, val, n FROM vum_base;
CREATE TRIGGER vum_ti INSTEAD OF INSERT ON vum_trigview
  FOR EACH ROW EXECUTE FUNCTION vum_trg();

INSERT INTO vum_trigview (id, val, n) VALUES (200,'t',1);

-- No INSTEAD OF DELETE trigger, so the DELETE is still refused.
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view "vum_trigview"
-- end-expected-error
DELETE FROM vum_trigview WHERE id = 1;

-- begin-expected
-- columns: is_updatable,is_insertable_into,is_trigger_updatable,is_trigger_deletable,is_trigger_insertable_into
-- row: NO|NO|NO|NO|YES
-- end-expected
SELECT is_updatable, is_insertable_into, is_trigger_updatable, is_trigger_deletable,
       is_trigger_insertable_into
  FROM information_schema.views WHERE table_name = 'vum_trigview';

DROP VIEW vum_trigview CASCADE;
DROP FUNCTION vum_trg() CASCADE;

-- ---------------------------------------------------------------------------
-- pg_settings: each setting answers for itself
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: name,vartype,context,unit,category
-- row: array_nulls|bool|user||Version and Platform Compatibility / Previous PostgreSQL Versions
-- row: block_size|integer|internal||Preset Options
-- row: constraint_exclusion|enum|user||Query Tuning / Other Planner Options
-- row: huge_pages|enum|postmaster||Resource Usage / Memory
-- row: work_mem|integer|user|kB|Resource Usage / Memory
-- end-expected
SELECT name, vartype, context, coalesce(unit,'') AS unit, category FROM pg_settings
 WHERE name IN ('array_nulls','work_mem','block_size','constraint_exclusion','huge_pages')
 ORDER BY name;

-- extra_desc: the sentence that says what 0 or -1 means. It was null for every setting.
-- begin-expected
-- columns: name,extra_desc
-- row: DateStyle|Also controls interpretation of ambiguous date inputs.
-- row: lock_timeout|0 disables the timeout.
-- row: maintenance_work_mem|This includes operations such as VACUUM and CREATE INDEX.
-- row: statement_timeout|0 disables the timeout.
-- row: work_mem|This much memory can be used by each internal sort operation and hash table before switching to temporary disk files.
-- end-expected
SELECT name, replace(extra_desc, chr(10), '~') AS extra_desc FROM pg_settings
 WHERE name IN ('work_mem','statement_timeout','lock_timeout','maintenance_work_mem','DateStyle')
 ORDER BY name;

-- begin-expected
-- columns: name,extra_desc
-- row: array_nulls|When turned on, unquoted NULL in an array input value means a null value; otherwise it is taken literally.
-- end-expected
SELECT name, replace(extra_desc, chr(10), '~') AS extra_desc FROM pg_settings
 WHERE name = 'array_nulls';

-- A setting PostgreSQL leaves without one keeps it null.
-- begin-expected
-- columns: name,has_extra
-- row: block_size|f
-- row: enable_bitmapscan|f
-- end-expected
SELECT name, (extra_desc IS NOT NULL) AS has_extra FROM pg_settings
 WHERE name IN ('block_size','enable_bitmapscan') ORDER BY name;

-- boot_val is the compiled-in default: it does not vary with the machine, which is precisely
-- why a client comparing two servers reads it. The flush-after parameters are left out: what
-- PostgreSQL compiles in for those depends on whether the platform can ask for a flush at all, so
-- it boots them at 0 on Windows and at 64 and 32 on Linux, and neither answer is the wrong one.
-- begin-expected
-- columns: name,boot_val
-- row: application_name|
-- row: client_encoding|SQL_ASCII
-- row: lc_monetary|C
-- row: lc_numeric|C
-- row: lc_time|C
-- row: max_stack_depth|100
-- row: server_encoding|SQL_ASCII
-- row: wal_buffers|-1
-- end-expected
SELECT name, boot_val FROM pg_settings
 WHERE name IN ('application_name','client_encoding','server_encoding','lc_monetary',
                'lc_numeric','lc_time','max_stack_depth','wal_buffers')
 ORDER BY name;

-- begin-expected
-- columns: name,boot_val
-- row: TimeZone|GMT
-- end-expected
SELECT name, boot_val FROM pg_settings WHERE name = 'TimeZone';

-- The planner knobs a test suite actually SETs, with PostgreSQL's own bounds.
-- begin-expected
-- columns: name,setting,vartype,min_val,max_val
-- row: default_statistics_target|100|integer|1|10000
-- row: from_collapse_limit|8|integer|1|2147483647
-- row: geqo_threshold|12|integer|2|2147483647
-- row: join_collapse_limit|8|integer|1|2147483647
-- end-expected
SELECT name, setting, vartype, min_val, max_val FROM pg_settings
 WHERE name IN ('default_statistics_target','from_collapse_limit','join_collapse_limit',
                'geqo_threshold')
 ORDER BY name;

-- begin-expected
-- columns: name,setting,vartype
-- row: enable_bitmapscan|on|bool
-- row: enable_indexonlyscan|on|bool
-- row: enable_material|on|bool
-- row: enable_memoize|on|bool
-- row: enable_partition_pruning|on|bool
-- row: enable_partitionwise_aggregate|off|bool
-- row: enable_sort|on|bool
-- row: enable_tidscan|on|bool
-- end-expected
SELECT name, setting, vartype FROM pg_settings
 WHERE name IN ('enable_bitmapscan','enable_indexonlyscan','enable_sort','enable_material',
                'enable_memoize','enable_tidscan','enable_partition_pruning',
                'enable_partitionwise_aggregate')
 ORDER BY name;

-- begin-expected
-- columns: name,setting,vartype,enumvals
-- row: constraint_exclusion|partition|enum|{partition,on,off}
-- row: log_error_verbosity|default|enum|{terse,default,verbose}
-- row: stats_fetch_consistency|cache|enum|{none,cache,snapshot}
-- row: xmlbinary|base64|enum|{base64,hex}
-- end-expected
SELECT name, setting, vartype, enumvals::text AS enumvals FROM pg_settings
 WHERE name IN ('constraint_exclusion','xmlbinary','log_error_verbosity','stats_fetch_consistency')
 ORDER BY name;

-- ---------------------------------------------------------------------------
-- SET, SHOW, current_setting and the pg_settings row are one answer
-- ---------------------------------------------------------------------------

SET enable_sort = off;

-- begin-expected
-- columns: enable_sort
-- row: off
-- end-expected
SHOW enable_sort;

-- begin-expected
-- columns: cur,val,src,rst
-- row: off|off|session|on
-- end-expected
SELECT current_setting('enable_sort') AS cur, s.setting AS val, s.source AS src,
       s.reset_val AS rst
  FROM pg_settings s WHERE s.name = 'enable_sort';

RESET enable_sort;

-- begin-expected
-- columns: cur,src
-- row: on|default
-- end-expected
SELECT current_setting('enable_sort') AS cur, s.source AS src
  FROM pg_settings s WHERE s.name = 'enable_sort';

-- A setting is assigned only what its own metadata allows.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: parameter "enable_sort" requires a Boolean value
-- end-expected-error
SET enable_sort = 'maybe';

-- begin-expected-error
-- sqlstate: 22023
-- message-like: 20000 is outside the valid range for parameter "default_statistics_target" (1 .. 10000)
-- end-expected-error
SET default_statistics_target = 20000;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "constraint_exclusion": "sometimes"
-- end-expected-error
SET constraint_exclusion = 'sometimes';

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "port" cannot be changed without restarting the server
-- end-expected-error
SET port = 6000;

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "max_connections" cannot be changed without restarting the server
-- end-expected-error
SET max_connections = 500;

-- A parameter that does not exist behaves the way PostgreSQL makes it behave.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "totally_bogus_guc"
-- end-expected-error
SET totally_bogus_guc = 5;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "totally_bogus_guc"
-- end-expected-error
SELECT set_config('totally_bogus_guc','7',false);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "totally_bogus_guc"
-- end-expected-error
SHOW totally_bogus_guc;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_settings WHERE name = 'totally_bogus_guc';

-- A qualified custom parameter is accepted and readable, but PostgreSQL keeps it out of
-- pg_settings until an extension declares it.
SET myapp.zzz = 'q';

-- begin-expected
-- columns: cur,n
-- row: q|0
-- end-expected
SELECT current_setting('myapp.zzz') AS cur,
       (SELECT count(*) FROM pg_settings WHERE name = 'myapp.zzz') AS n;

-- begin-expected
-- columns: v
-- row: 5
-- end-expected
SELECT set_config('myapp.qqq','5',false) AS v;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_settings WHERE name = 'myapp.qqq';

-- A transaction's own settings are imposed by the transaction rather than chosen by the session,
-- but which of the two pg_settings names depends on whether the client has ever set the isolation
-- level on this connection -- a driver that calls setTransactionIsolation makes it 'session'. So
-- what is pinned here is the setting itself, and the source of two settings this harness does set
-- on its own connection, which both engines then report the same way.
-- begin-expected
-- columns: name,setting
-- row: transaction_isolation|read committed
-- end-expected
SELECT name, setting FROM pg_settings WHERE name = 'transaction_isolation';

-- begin-expected
-- columns: name,source
-- row: statement_timeout|session
-- end-expected
SELECT name, source FROM pg_settings WHERE name = 'statement_timeout';

-- begin-expected
-- columns: name,source
-- row: default_transaction_isolation|session
-- end-expected
SELECT name, source FROM pg_settings WHERE name = 'default_transaction_isolation';

DROP VIEW vum_nested, vum_srf, vum_union, vum_agg, vum_window, vum_with,
          vum_offset, vum_limit, vum_having, vum_group, vum_distinct,
          vum_const, vum_expr, vum_star, vum_order, vum_where, vum_simple CASCADE;
DROP TABLE vum_base CASCADE;
