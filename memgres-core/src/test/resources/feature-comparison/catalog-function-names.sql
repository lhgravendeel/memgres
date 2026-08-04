-- Which functions pg_proc says this server has, and what it says they take and return.
--
-- A catalog row is a claim a client acts on: pgjdbc answers DatabaseMetaData.getFunctions from
-- pg_proc, ::regproc resolves through it, psql's \df reads it, and a code generator writes calls
-- from it. A name listed there that PostgreSQL has nowhere is worse than a missing one, because
-- the client writes SQL the real server rejects. Every expected value below was read from
-- PostgreSQL 18.

-- The type I/O functions PostgreSQL actually has. Their names do not follow one rule: the short
-- bootstrap types and the reg* family run the suffix straight on, the longer names take an
-- underscore, and the two BRIN summary types drop the pg_ their type name carries. Building them
-- by appending "_in" to the type name gave fifty-six names PostgreSQL has nowhere.
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'char_in';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'charin';

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'tid_in';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'tidin';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'xid8in';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'regrolein';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'regoperatorin';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'regdictionaryin';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'brin_bloom_summary_in';

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'brin_minmax_multi_summary_send';

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'pg_brin_bloom_summary_in';

-- No reg* type has an input function spelled with an underscore.
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname LIKE 'reg%\_in';

-- pg_type points at the names pg_proc carries, so following the column reaches a function.
-- begin-expected
-- columns: a
-- row: xid8in
-- end-expected
SELECT typinput::text AS a FROM pg_type WHERE typname = 'xid8';

-- begin-expected
-- columns: a
-- row: regroleout
-- end-expected
SELECT typoutput::text AS a FROM pg_type WHERE typname = 'regrole';

-- begin-expected
-- columns: a
-- row: brin_bloom_summary_recv
-- end-expected
SELECT typreceive::text AS a FROM pg_type WHERE typname = 'pg_brin_bloom_summary';

-- begin-expected
-- columns: a
-- row: tidsend
-- end-expected
SELECT typsend::text AS a FROM pg_type WHERE typname = 'tid';

-- begin-expected
-- columns: a
-- row: pg_lsn_in
-- end-expected
SELECT typinput::text AS a FROM pg_type WHERE typname = 'pg_lsn';

-- A handler pseudo-type has no binary I/O at all.
-- begin-expected
-- columns: a
-- row: -
-- end-expected
SELECT typreceive::text AS a FROM pg_type WHERE typname = 'index_am_handler';

-- ... and the names resolve as a regproc, which is what a tool following the column does.
-- begin-expected
-- columns: a
-- row: charin
-- end-expected
SELECT 'charin'::regproc::text AS a;

-- begin-expected
-- columns: a
-- row: tidin
-- end-expected
SELECT 'tidin'::regproc::text AS a;

-- The geometric aliases memgres adds are its own; PostgreSQL has no row for any of them, and a
-- client that read one out of pg_catalog would write a call the real server refuses.
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname IN
  ('closest_point','intersects','is_horizontal','is_vertical','is_parallel','is_perpendicular');

-- Nor for these four. merge_action is a parser construct PostgreSQL evaluates with no pg_proc row
-- of its own; the other three PostgreSQL simply does not have.
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname IN
  ('sha1','delete_key','pg_advisory_xact_unlock','merge_action');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sha1(bytea) does not exist
-- end-expected-error
SELECT sha1('a'::bytea);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_advisory_xact_unlock(integer) does not exist
-- end-expected-error
SELECT pg_advisory_xact_unlock(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function delete_key(unknown, unknown) does not exist
-- end-expected-error
SELECT delete_key('a=>1', 'a');

-- The neighbours of those refusals still resolve.
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (sha256('a'::bytea) IS NOT NULL)::text AS a;

-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT area(box(point(0,0),point(2,2)))::text AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT isclosed(path '((0,0),(1,1),(2,0))')::text AS a;

-- An extension's functions live in the schema the extension was installed into, and nowhere at
-- all before CREATE EXTENSION runs. PostgreSQL never puts them in pg_catalog.
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname IN
  ('uuid_generate_v1','uuid_generate_v3','uuid_generate_v4','uuid_generate_v5',
   'uuid_nil','uuid_ns_dns','uuid_ns_url','show_trgm','similarity');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function uuid_generate_v4() does not exist
-- end-expected-error
SELECT uuid_generate_v4();

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function similarity(unknown, unknown) does not exist
-- end-expected-error
SELECT similarity('abc','abd');

-- Every aggregate the catalog reports has a pg_aggregate row behind it. This is what stops the
-- thirteen aggregates memgres evaluates and does not list -- any_value, mode, the two percentile_*
-- and the nine regr_* -- from simply being given a pg_proc row: a prokind='a' row with nothing
-- behind it drops out of every join a tool makes to find out how the aggregate works.
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*)::text AS a FROM pg_proc p WHERE p.prokind = 'a'
  AND NOT EXISTS (SELECT 1 FROM pg_aggregate ag WHERE ag.aggfnoid = p.oid);

-- The aggregates that are listed carry a return type per overload, not one polymorphic type for
-- the lot: sum answers six different types across its eight forms.
-- begin-expected
-- columns: a
-- row: bigint,double precision,interval,money,numeric,real
-- end-expected
SELECT string_agg(DISTINCT prorettype::regtype::text, ',' ORDER BY prorettype::regtype::text) AS a
  FROM pg_proc WHERE proname = 'sum';

-- begin-expected
-- columns: a
-- row: double precision,interval,numeric
-- end-expected
SELECT string_agg(DISTINCT prorettype::regtype::text, ',' ORDER BY prorettype::regtype::text) AS a
  FROM pg_proc WHERE proname = 'avg';

-- The record-returning functions that work in FROM position and were listed nowhere.
-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname IN
  ('json_to_record','jsonb_to_record','json_to_recordset','jsonb_to_recordset');

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname IN
  ('pg_options_to_table','pg_show_all_settings','pg_available_extension_versions');

-- begin-expected
-- columns: a
-- row: record
-- end-expected
SELECT prorettype::regtype::text AS a FROM pg_proc WHERE proname = 'json_to_record';

-- One of these returns a set and the other does not, which is the whole difference between them.
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT proretset::text AS a FROM pg_proc WHERE proname = 'jsonb_to_record';

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT proretset::text AS a FROM pg_proc WHERE proname = 'jsonb_to_recordset';

-- ... and they run.
-- begin-expected
-- columns: a
-- row: 7
-- end-expected
SELECT any_value(x)::text AS a FROM (VALUES (7),(8)) t(x);

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT (mode() WITHIN GROUP (ORDER BY x))::text AS a FROM (VALUES (1),(1),(2)) t(x);

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT (percentile_disc(0.5) WITHIN GROUP (ORDER BY x))::text AS a
  FROM (VALUES (1),(2),(3)) t(x);

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT regr_count(y,x)::text AS a FROM (VALUES (1.0,1.0),(2.0,2.0)) t(y,x);

-- begin-expected
-- columns: a
-- row: 0.3333333333333333
-- end-expected
SELECT regr_slope(y,x)::text AS a FROM (VALUES (1.0,1.0),(2.0,4.0)) t(y,x);

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (count(*) >= 0)::text AS a FROM json_to_recordset('[{"a":1}]') AS t(a int);

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (count(*) >= 0)::text AS a FROM pg_options_to_table(ARRAY['fillfactor=70']);

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (count(*) >= 0)::text AS a FROM pg_show_all_settings();

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (count(*) >= 0)::text AS a FROM pg_available_extension_versions();

-- The signature columns themselves. An event trigger helper returns a record or an oid, not the
-- void memgres reported for all four of them.
-- begin-expected
-- columns: a
-- row: oid
-- end-expected
SELECT prorettype::regtype::text AS a FROM pg_proc
  WHERE proname = 'pg_event_trigger_table_rewrite_oid';

-- begin-expected
-- columns: a
-- row: record/true
-- end-expected
SELECT prorettype::regtype::text || '/' || proretset::text AS a FROM pg_proc
  WHERE proname = 'pg_event_trigger_ddl_commands';

-- begin-expected
-- columns: a
-- row: smallint
-- end-expected
SELECT prorettype::regtype::text AS a FROM pg_proc WHERE proname = 'uuid_extract_version';

-- A table access method's handler answers table_am_handler; the index ones answer index_am_handler.
-- begin-expected
-- columns: a
-- row: table_am_handler
-- end-expected
SELECT prorettype::regtype::text AS a FROM pg_proc WHERE proname = 'heap_tableam_handler';

-- begin-expected
-- columns: a
-- row: index_am_handler
-- end-expected
SELECT prorettype::regtype::text AS a FROM pg_proc WHERE proname = 'bthandler';

-- pg_control_system reads the control file once and answers one row.
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT proretset::text AS a FROM pg_proc WHERE proname = 'pg_control_system';

-- A parameter with a default is one the caller may leave out, and pg_proc counts them.
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT pronargdefaults::text AS a FROM pg_proc WHERE proname = 'jsonb_set';

-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT proisstrict::text AS a FROM pg_proc WHERE proname = 'pg_stat_reset_shared';

-- begin-expected
-- columns: a
-- row: 2276
-- end-expected
SELECT provariadic::text AS a FROM pg_proc WHERE proname = 'concat';

-- The polymorphic I/O functions are declared over the kind of type they serve, not over the one
-- type whose pg_type row happened to reach them, and they read a type OID and a typmod besides.
-- begin-expected
-- columns: a
-- row: anyarray/3
-- end-expected
SELECT prorettype::regtype::text || '/' || pronargs::text AS a FROM pg_proc
  WHERE proname = 'array_in';

-- begin-expected
-- columns: a
-- row: record/3
-- end-expected
SELECT prorettype::regtype::text || '/' || pronargs::text AS a FROM pg_proc
  WHERE proname = 'record_recv';

-- begin-expected
-- columns: a
-- row: "char"/1
-- end-expected
SELECT prorettype::regtype::text || '/' || pronargs::text AS a FROM pg_proc
  WHERE proname = 'charin';

-- oid(bigint) returns oid. The first cast that names it is int8 -> regproc, and reading the
-- target type off that cast said the function returned regproc.
-- begin-expected
-- columns: a
-- row: oid
-- end-expected
SELECT prorettype::regtype::text AS a FROM pg_proc WHERE proname = 'oid';

-- tsquery_phrase has a two-argument form and a three-argument one; the <-> operator names only
-- the first, and deriving the row from the operator lost the other.
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT count(*)::text AS a FROM pg_proc WHERE proname = 'tsquery_phrase';

-- Whatever a row says it takes, it says how many: pronargs is the length of proargtypes on every
-- row of the catalog.
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT bool_and(pronargs = array_length(proargtypes, 1))::text AS a FROM pg_proc
  WHERE pronargs > 0;
