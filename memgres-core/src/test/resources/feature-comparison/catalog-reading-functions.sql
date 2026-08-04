-- The functions that read pg_catalog back out: pg_get_functiondef and its family, the
-- regprocedure name resolvers, and the closest-point operator behind close_lseg / close_sb /
-- close_ls.
--
-- These are the functions a tool uses to turn a catalog row into something it can act on. psql's
-- \df+ prints pg_get_functiondef, a migration tool round-trips a signature through regprocedure,
-- and a client that reads pg_proc and then writes a call gets its argument list from
-- pg_get_function_arguments. An empty string where PostgreSQL prints a definition is not a
-- harmless blank: it tells the tool the function has no body rather than that this server does
-- not know it.
--
-- Every expected value below was read from PostgreSQL 18 on the reference server. Nothing here
-- counts catalog rows -- every case pins one named row and reads named columns of it, because a
-- count differs between that server and memgres for reasons neither engine is wrong about.

-- ============================================================================
-- pg_get_functiondef of a built-in, deparsed from its pg_proc row
-- ============================================================================

-- Newlines cannot appear in a -- row: line, so the definition is folded on chr(10).
-- begin-expected
-- columns: def
-- row: CREATE OR REPLACE FUNCTION pg_catalog.upper(text)~ RETURNS text~ LANGUAGE internal~ IMMUTABLE PARALLEL SAFE STRICT~AS $function$upper$function$~
-- end-expected
SELECT replace(pg_get_functiondef('pg_catalog.upper(text)'::regprocedure), chr(10), '~') AS def;

-- A stable function prints STABLE where an immutable one prints IMMUTABLE, and VOLATILE is the
-- default PostgreSQL leaves off entirely.
-- begin-expected
-- columns: def
-- row: CREATE OR REPLACE FUNCTION pg_catalog.now()~ RETURNS timestamp with time zone~ LANGUAGE internal~ STABLE PARALLEL SAFE STRICT~AS $function$now$function$~
-- end-expected
SELECT replace(pg_get_functiondef('pg_catalog.now()'::regprocedure), chr(10), '~') AS def;

-- begin-expected
-- columns: def
-- row: CREATE OR REPLACE FUNCTION pg_catalog.current_database()~ RETURNS name~ LANGUAGE internal~ STABLE PARALLEL SAFE STRICT~AS $function$current_database$function$~
-- end-expected
SELECT replace(pg_get_functiondef('pg_catalog.current_database()'::regprocedure), chr(10), '~') AS def;

-- begin-expected
-- columns: def
-- row: CREATE OR REPLACE FUNCTION pg_catalog.initcap(text)~ RETURNS text~ LANGUAGE internal~ IMMUTABLE PARALLEL SAFE STRICT~AS $function$initcap$function$~
-- end-expected
SELECT replace(pg_get_functiondef('pg_catalog.initcap(text)'::regprocedure), chr(10), '~') AS def;

-- begin-expected
-- columns: def
-- row: CREATE OR REPLACE FUNCTION pg_catalog.chr(integer)~ RETURNS text~ LANGUAGE internal~ IMMUTABLE PARALLEL SAFE STRICT~AS $function$chr$function$~
-- end-expected
SELECT replace(pg_get_functiondef('pg_catalog.chr(integer)'::regprocedure), chr(10), '~') AS def;

-- begin-expected
-- columns: def
-- row: CREATE OR REPLACE FUNCTION pg_catalog.ascii(text)~ RETURNS integer~ LANGUAGE internal~ IMMUTABLE PARALLEL SAFE STRICT~AS $function$ascii$function$~
-- end-expected
SELECT replace(pg_get_functiondef('pg_catalog.ascii(text)'::regprocedure), chr(10), '~') AS def;

-- begin-expected
-- columns: def
-- row: CREATE OR REPLACE FUNCTION pg_catalog.lower(text)~ RETURNS text~ LANGUAGE internal~ IMMUTABLE PARALLEL SAFE STRICT~AS $function$lower$function$~
-- end-expected
SELECT replace(pg_get_functiondef('pg_catalog.lower(text)'::regprocedure), chr(10), '~') AS def;

-- An OID no pg_proc row carries has no definition to deparse, and the answer is NULL rather than
-- an empty definition a client would read as a function with no body.
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_functiondef(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_functiondef(999999999) AS a;

-- An aggregate has no function definition at all, and PostgreSQL refuses rather than printing
-- something that would not run.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "sum" is an aggregate function
-- end-expected-error
SELECT pg_get_functiondef('pg_catalog.sum(integer)'::regprocedure);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "array_agg" is an aggregate function
-- end-expected-error
SELECT pg_get_functiondef('pg_catalog.array_agg(anyarray)'::regprocedure);

-- ============================================================================
-- pg_get_function_arguments / _result / _identity_arguments
-- ============================================================================

-- begin-expected
-- columns: args
-- row: text, integer, text
-- end-expected
SELECT pg_get_function_arguments('pg_catalog.lpad(text,integer,text)'::regprocedure) AS args;

-- begin-expected
-- columns: args
-- row: text, integer, text
-- end-expected
SELECT pg_get_function_identity_arguments('pg_catalog.lpad(text,integer,text)'::regprocedure) AS args;

-- begin-expected
-- columns: res
-- row: text
-- end-expected
SELECT pg_get_function_result('pg_catalog.lpad(text,integer,text)'::regprocedure) AS res;

-- A set-returning function prints SETOF in front of its element type.
-- begin-expected
-- columns: res
-- row: SETOF integer
-- end-expected
SELECT pg_get_function_result('pg_catalog.generate_series(integer,integer)'::regprocedure) AS res;

-- begin-expected
-- columns: args
-- row: bytea
-- end-expected
SELECT pg_get_function_arguments('pg_catalog.md5(bytea)'::regprocedure) AS args;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_function_arguments(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_function_result(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_function_identity_arguments(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_function_result(99999) AS a;

-- A join over the catalog still answers, which is how a client reads these: one row per named
-- function, not a count.
-- begin-expected
-- columns: proname,res
-- row: initcap|text
-- end-expected
SELECT p.proname, pg_get_function_result(p.oid) AS res
FROM pg_proc p
WHERE p.proname = 'initcap' AND p.pronamespace = 'pg_catalog'::regnamespace
ORDER BY 1, 2;

-- begin-expected
-- columns: nspname,proname,args
-- row: pg_catalog|chr|integer
-- end-expected
SELECT n.nspname, p.proname, pg_get_function_arguments(p.oid) AS args
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE p.proname = 'chr' AND n.nspname = 'pg_catalog'
ORDER BY 1, 2, 3;

-- ============================================================================
-- The rest of the pg_get_*def family answers NULL the same way
-- ============================================================================

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_indexdef(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_indexdef(0, 1, true) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_constraintdef(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_viewdef(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_viewdef(0, true) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_ruledef(0) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_triggerdef(0) AS a;

-- A NULL argument answers NULL rather than an empty definition.
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_indexdef(NULL) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_constraintdef(NULL) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_viewdef(NULL) AS a;

-- A relation that exists but is not an index, and not a view, has no definition of either kind.
CREATE TABLE b5cr_plain (a integer);

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_indexdef('b5cr_plain'::regclass) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT pg_get_viewdef('b5cr_plain'::regclass) AS a;

DROP TABLE b5cr_plain;

-- ============================================================================
-- regprocedure and to_regprocedure resolve against pg_proc
-- ============================================================================

-- begin-expected
-- columns: a
-- row: upper(text)
-- end-expected
SELECT 'pg_catalog.upper(text)'::regprocedure::text AS a;

-- The argument types are compared by OID, so every alias PostgreSQL accepts names the same row and
-- the answer comes back in the canonical spelling.
-- begin-expected
-- columns: a
-- row: abs(integer)
-- end-expected
SELECT 'pg_catalog.abs(int4)'::regprocedure::text AS a;

-- begin-expected
-- columns: a
-- row: upper(text)
-- end-expected
SELECT to_regprocedure('pg_catalog.upper(text)')::text AS a;

-- begin-expected
-- columns: a
-- row: upper(text)
-- end-expected
SELECT to_regprocedure('pg_catalog.UPPER(TEXT)')::text AS a;

-- begin-expected
-- columns: a
-- row: generate_series(integer,integer)
-- end-expected
SELECT to_regprocedure('pg_catalog.generate_series(integer,integer)')::text AS a;

-- An overload that is not there is not the function with the same name: upper takes text, not
-- character varying, and to_regprocedure answers NULL rather than the nearest match.
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT to_regprocedure('pg_catalog.upper(character varying)')::text AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT to_regprocedure('pg_catalog.b5nosuchfn(text)')::text AS a;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function "b5nosuchfn(text)" does not exist
-- end-expected-error
SELECT 'b5nosuchfn(text)'::regprocedure::text;

-- A bare name several rows answer to names no one function, and to_regproc answers NULL rather
-- than picking one -- upper has three overloads and md5 has two.
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT to_regproc('upper')::text AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT to_regproc('md5')::text AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT to_regproc('b5nosuchfn')::text AS a;

-- begin-expected-error
-- sqlstate: 42725
-- message-like: more than one function named "upper"
-- end-expected-error
SELECT 'upper'::regproc::text;

-- ============================================================================
-- The closest-point operator ## and the three names behind it
-- ============================================================================

-- Two segments that cross answer with the crossing.
-- begin-expected
-- columns: a
-- row: (1,1)
-- end-expected
SELECT (lseg '[(0,0),(2,2)]' ## lseg '[(0,2),(2,0)]')::text AS a;

-- Two segments that run parallel have no closest point in PostgreSQL at all, whether they are
-- apart, alongside or collinear, and the answer is NULL.
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT (lseg '[(0,0),(1,1)]' ## lseg '[(2,0),(3,1)]')::text AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT (lseg '[(0,0),(1,0)]' ## lseg '[(0,1),(1,1)]')::text AS a;

-- Collinear and touching at an endpoint is still parallel.
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT (lseg '[(0,0),(2,0)]' ## lseg '[(2,0),(4,0)]')::text AS a;

-- The result lies on the second operand, which is what makes it the closest point to the first.
-- begin-expected
-- columns: a
-- row: (5,1)
-- end-expected
SELECT (lseg '[(0,0),(1,0)]' ## lseg '[(5,1),(5,3)]')::text AS a;

-- begin-expected
-- columns: a
-- row: (1,0)
-- end-expected
SELECT close_lseg(lseg '[(0,0),(4,0)]', lseg '[(1,-1),(1,1)]')::text AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT close_lseg(lseg '[(0,0),(1,1)]', lseg '[(0,1),(1,2)]')::text AS a;

-- A segment that meets the box is answered with the point of the segment nearest the box centre,
-- not with a corner.
-- begin-expected
-- columns: a
-- row: (2,1)
-- end-expected
SELECT close_sb(lseg '[(-1,1),(5,1)]', box '(0,0),(4,2)')::text AS a;

-- begin-expected
-- columns: a
-- row: (2,0.5)
-- end-expected
SELECT close_sb(lseg '[(-1,0.5),(5,0.5)]', box '(0,0),(4,2)')::text AS a;

-- begin-expected
-- columns: a
-- row: (1,0.5)
-- end-expected
SELECT close_sb(lseg '[(-1,0.5),(1,0.5)]', box '(0,0),(4,2)')::text AS a;

-- A segment entirely inside the box is answered the same way.
-- begin-expected
-- columns: a
-- row: (3,0.2)
-- end-expected
SELECT close_sb(lseg '[(3,0.2),(3.5,0.3)]', box '(0,0),(4,2)')::text AS a;

-- A segment clear of the box is answered with a point on the box.
-- begin-expected
-- columns: a
-- row: (0,1)
-- end-expected
SELECT close_sb(lseg '[(-3,1),(-1,1)]', box '(0,0),(4,2)')::text AS a;

-- begin-expected
-- columns: a
-- row: (4,1)
-- end-expected
SELECT close_sb(lseg '[(6,1),(8,1)]', box '(0,0),(4,2)')::text AS a;

-- begin-expected
-- columns: a
-- row: (0,2)
-- end-expected
SELECT close_sb(lseg '[(-1,3),(5,3)]', box '(0,0),(4,2)')::text AS a;

-- begin-expected
-- columns: a
-- row: (4,2)
-- end-expected
SELECT close_sb(lseg '[(6,3),(7,4)]', box '(0,0),(4,2)')::text AS a;

-- begin-expected
-- columns: a
-- row: (2,1)
-- end-expected
SELECT (lseg '[(-1,1),(5,1)]' ## box '(0,0),(4,2)')::text AS a;

-- The box is never the first operand of ##: the result has to lie on the second one.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: box ## lseg
-- end-expected-error
SELECT (box '(0,0),(4,2)' ## lseg '[(-1,1),(5,1)]')::text;

-- A line that crosses a segment answers with the crossing.
-- begin-expected
-- columns: a
-- row: (0.5,5)
-- end-expected
SELECT close_ls(line '{0,1,-5}', lseg '[(0,0),(1,10)]')::text AS a;

-- A line that misses the segment answers with the endpoint nearer the line.
-- begin-expected
-- columns: a
-- row: (1,1)
-- end-expected
SELECT close_ls(line '{0,1,-5}', lseg '[(0,0),(1,1)]')::text AS a;

-- begin-expected
-- columns: a
-- row: (3,0)
-- end-expected
SELECT close_ls(line '{1,-1,0}', lseg '[(3,0),(5,1)]')::text AS a;

-- A line parallel to the segment has no closest point on it.
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT close_ls(line '{0,1,-5}', lseg '[(0,6),(1,6)]')::text AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT close_ls(line '{1,-1,0}', lseg '[(0,1),(1,2)]')::text AS a;

-- begin-expected
-- columns: a
-- row: (2,2)
-- end-expected
SELECT (line '{1,-1,0}' ## lseg '[(0,2),(2,2)]')::text AS a;

-- The four spellings that already agreed still agree.
-- begin-expected
-- columns: a
-- row: (1,1)
-- end-expected
SELECT close_ps(point '(0,0)', lseg '[(1,1),(2,2)]')::text AS a;

-- begin-expected
-- columns: a
-- row: (2,2)
-- end-expected
SELECT close_pb(point '(3,3)', box '(0,0),(2,2)')::text AS a;

-- begin-expected
-- columns: a
-- row: (0,0)
-- end-expected
SELECT close_pl(point '(0,0)', line '{1,-1,0}')::text AS a;

-- ============================================================================
-- Recovery control names that pg_proc listed and the executor could not dispatch
-- ============================================================================

-- begin-expected-error
-- sqlstate: 55000
-- message-like: recovery is not in progress
-- end-expected-error
SELECT pg_wal_replay_pause();

-- begin-expected-error
-- sqlstate: 55000
-- message-like: recovery is not in progress
-- end-expected-error
SELECT pg_wal_replay_resume();

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (pg_switch_wal() IS NOT NULL) AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (pg_create_restore_point('b5_catalog_reading') IS NOT NULL) AS a;

-- Declared strict, so a NULL name answers NULL.
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (pg_create_restore_point(NULL) IS NULL) AS a;
