-- ============================================================================
-- Feature Comparison: strict functions, timestamp range, ALTER that records nothing
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Every text-search function PostgreSQL exposes is declared strict, so a NULL
-- argument makes the whole call NULL without the body running. The two ways of
-- getting that wrong are both worse than an ordinary wrong answer: the body
-- fails on the NULL and raises XX000, a fault code no client can act on, or it
-- stringifies the NULL and the query goes on with the four characters "null"
-- where a value should be.
--
-- A timestamp holds 4714-11-24 BC to 294276-12-31 — narrower at the top than
-- date, because it spends bits on the time of day. Arithmetic that lands outside
-- that has no representable answer, and returning one anyway hands back a value
-- no PostgreSQL could store or send.
--
-- Several ALTER forms change nothing memgres records. PostgreSQL still checks
-- the schema, the role, the relation and the option name they refer to, and
-- reporting success for a move that did not happen leaves the next statement
-- failing somewhere unrelated.
-- ============================================================================

DROP SCHEMA IF EXISTS b7 CASCADE;

CREATE SCHEMA b7;

SET search_path = public;

-- ============================================================================
-- A strict function is never entered with a NULL argument
-- ============================================================================

-- a NULL tsvector in, a NULL out — not an internal error
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT setweight(NULL::tsvector, 'A') IS NULL AS r;

-- and a NULL in the second position counts the same
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT setweight('a:1'::tsvector, NULL) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ts_delete(NULL::tsvector, 'a') IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ts_delete('a:1'::tsvector, NULL) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ts_filter(NULL::tsvector, '{a}') IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT tsvector_to_array(NULL::tsvector) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT array_to_tsvector(NULL::text[]) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT numnode(NULL::tsquery) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT querytree(NULL::tsquery) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT tsquery_phrase(NULL::tsquery, 'a'::tsquery) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ts_rewrite(NULL::tsquery, 'a'::tsquery, 'b'::tsquery) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ts_rank_cd(NULL::tsvector, 'a'::tsquery) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT strip(NULL::tsvector) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT length(NULL::tsvector) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ts_lexize('simple', NULL) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (NULL::tsvector @@ 'a'::tsquery) IS NULL AS r;

-- the quiet direction: these three used to answer with the word "null"
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT phraseto_tsquery(NULL::text) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT plainto_tsquery(NULL::text) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT websearch_to_tsquery(NULL::text) IS NULL AS r;

-- and with a value they still do their job
-- begin-expected
-- columns: q
-- row: 'cat' <-> 'sat'
-- end-expected
SELECT phraseto_tsquery('cat sat')::text AS q;

-- begin-expected
-- columns: q
-- row: 'cat' & 'sat'
-- end-expected
SELECT plainto_tsquery('cat sat')::text AS q;

-- begin-expected
-- columns: q
-- row: 'cat' | 'sat'
-- end-expected
SELECT websearch_to_tsquery('cat or sat')::text AS q;

-- ============================================================================
-- A timestamp that no PostgreSQL could store is refused, not returned
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '294276-12-31 23:59:59' + interval '1 second';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '294276-12-31 00:00:00' + interval '1 day';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '294276-01-01' + interval '1 year';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '2000-01-01' + interval '2147483647 days';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '2000-01-01' - interval '2147483647 days';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '4714-11-24 BC' - interval '1 day';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '4714-11-24 BC' - interval '1 year';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamptz '294276-12-31 23:59:59+00' + interval '1 second';

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamptz '2000-01-01' + interval '2147483647 days';

-- date + interval is timestamp arithmetic, so it stops where timestamp does
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT date '2000-01-01' + interval '2147483647 days';

-- just inside the bound is untouched
-- begin-expected
-- columns: t
-- row: 294276-12-31 23:59:59.000001
-- end-expected
SELECT (timestamp '294276-12-31 23:59:59' + interval '1 microsecond')::text AS t;

-- begin-expected
-- columns: t
-- row: 294276-02-01 00:00:00
-- end-expected
SELECT (timestamp '294276-01-01' + interval '1 month')::text AS t;

-- date has its own, wider end, and adding days rather than an interval stays on it
-- begin-expected
-- columns: d
-- row: 4714-12-31 BC
-- end-expected
SELECT (date '4713-01-01 BC' - 1)::text AS d;

-- begin-expected
-- columns: d
-- row: 5874897-12-31
-- end-expected
SELECT (date '5874897-12-31')::text AS d;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date out of range
-- end-expected-error
SELECT date '5874897-12-31' + 1;

-- begin-expected
-- columns: a
-- row: 298989 years 1 mon 7 days
-- end-expected
SELECT age(timestamp '294276-12-31', timestamp '4714-11-24 BC')::text AS a;

-- an infinite factor stretches a finite span; zero times infinity is indeterminate
-- begin-expected
-- columns: i
-- row: infinity
-- end-expected
SELECT (interval '1 day' * 'Infinity'::float8)::text AS i;

-- begin-expected
-- columns: i
-- row: -infinity
-- end-expected
SELECT (interval '1 day' * '-Infinity'::float8)::text AS i;

-- begin-expected
-- columns: i
-- row: -infinity
-- end-expected
SELECT (interval '-1 day' * 'Infinity'::float8)::text AS i;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT interval '0 days' * 'Infinity'::float8;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT interval '1 day' * 'NaN'::float8;

-- begin-expected
-- columns: i
-- row: 00:00:00
-- end-expected
SELECT (interval '1 day' / 'Infinity'::float8)::text AS i;

-- begin-expected
-- columns: i
-- row: infinity
-- end-expected
SELECT (interval 'infinity' * 2)::text AS i;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT interval 'infinity' * 0;

-- ============================================================================
-- SET SCHEMA names a schema that has to exist
-- ============================================================================

CREATE TABLE b7_t (a int, b int);

CREATE AGGREGATE b7_ag (int) (SFUNC = int4pl, STYPE = int);

CREATE COLLATION b7_co (LOCALE = 'C');

CREATE TEXT SEARCH DICTIONARY b7_dict (TEMPLATE = simple);

CREATE TEXT SEARCH CONFIGURATION b7_cfg (COPY = simple);

CREATE STATISTICS b7_st ON a, b FROM b7_t;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema" does not exist
-- end-expected-error
ALTER AGGREGATE b7_ag(int) SET SCHEMA nosuchschema;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema" does not exist
-- end-expected-error
ALTER COLLATION b7_co SET SCHEMA nosuchschema;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema" does not exist
-- end-expected-error
ALTER TEXT SEARCH DICTIONARY b7_dict SET SCHEMA nosuchschema;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema" does not exist
-- end-expected-error
ALTER TEXT SEARCH CONFIGURATION b7_cfg SET SCHEMA nosuchschema;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema" does not exist
-- end-expected-error
ALTER STATISTICS b7_st SET SCHEMA nosuchschema;

-- a schema that is there is accepted, so the check refuses the missing one and no more
ALTER AGGREGATE b7_ag(int) SET SCHEMA b7;

ALTER STATISTICS b7_st SET SCHEMA b7;

-- ============================================================================
-- OWNER TO names a role that has to exist
-- ============================================================================

CREATE TYPE b7_ty AS ENUM ('a');

CREATE DOMAIN b7_dom AS int;

CREATE AGGREGATE b7_ag2 (int) (SFUNC = int4pl, STYPE = int);

CREATE COLLATION b7_co2 (LOCALE = 'C');

CREATE TEXT SEARCH DICTIONARY b7_dict2 (TEMPLATE = simple);

CREATE TEXT SEARCH CONFIGURATION b7_cfg2 (COPY = simple);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER TYPE b7_ty OWNER TO nosuchrole;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER DOMAIN b7_dom OWNER TO nosuchrole;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER AGGREGATE b7_ag2(int) OWNER TO nosuchrole;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER COLLATION b7_co2 OWNER TO nosuchrole;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER TEXT SEARCH DICTIONARY b7_dict2 OWNER TO nosuchrole;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER TEXT SEARCH CONFIGURATION b7_cfg2 OWNER TO nosuchrole;

-- ============================================================================
-- An operator is moved and re-owned like anything else
-- ============================================================================

CREATE FUNCTION b7_lt(int, int) RETURNS bool AS $$ SELECT $1 < $2 $$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR <^ (LEFTARG = int, RIGHTARG = int, FUNCTION = b7_lt);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema" does not exist
-- end-expected-error
ALTER OPERATOR <^ (int, int) SET SCHEMA nosuchschema;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER OPERATOR <^ (int, int) OWNER TO nosuchrole;

-- the refused ALTER changed nothing: the operator is still there and still usable
-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (1 <^ 2)::text AS r;

-- ============================================================================
-- A publication lists relations that have to exist
-- ============================================================================

CREATE TABLE b7_pa (i int);

CREATE TABLE b7_pb (i int);

CREATE PUBLICATION b7_pub FOR TABLE b7_pa;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "nosuchtable" does not exist
-- end-expected-error
ALTER PUBLICATION b7_pub ADD TABLE nosuchtable;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "nosuchtable" does not exist
-- end-expected-error
ALTER PUBLICATION b7_pub DROP TABLE nosuchtable;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "nosuchtable" does not exist
-- end-expected-error
ALTER PUBLICATION b7_pub SET TABLE nosuchtable;

-- a relation already listed would be listed twice
-- begin-expected-error
-- sqlstate: 42710
-- message-like: relation "b7_pa" is already member of publication "b7_pub"
-- end-expected-error
ALTER PUBLICATION b7_pub ADD TABLE b7_pa;

-- one that is not listed cannot be removed
-- begin-expected-error
-- sqlstate: 42704
-- message-like: relation "b7_pb" is not part of the publication
-- end-expected-error
ALTER PUBLICATION b7_pub DROP TABLE b7_pb;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER PUBLICATION b7_pub OWNER TO nosuchrole;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized publication parameter: "nosuchoption"
-- end-expected-error
ALTER PUBLICATION b7_pub SET (nosuchoption = true);

-- the options it does take are still taken
ALTER PUBLICATION b7_pub SET (publish = 'insert');

ALTER PUBLICATION b7_pub ADD TABLE b7_pb;

-- begin-expected
-- columns: tablename
-- row: b7_pa
-- row: b7_pb
-- end-expected
SELECT tablename::text FROM pg_publication_tables WHERE pubname = 'b7_pub' ORDER BY 1;

-- ============================================================================
-- A policy rename and its roles are checked
-- ============================================================================

CREATE TABLE b7_rls (a int);

CREATE POLICY b7_p1 ON b7_rls USING (a > 0);

CREATE POLICY b7_p2 ON b7_rls USING (a > 0);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: policy "b7_p2" for table "b7_rls" already exists
-- end-expected-error
ALTER POLICY b7_p1 ON b7_rls RENAME TO b7_p2;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "nosuchrole" does not exist
-- end-expected-error
ALTER POLICY b7_p1 ON b7_rls TO nosuchrole;

-- both survive the refusals: the rename used to succeed and take the name away
-- begin-expected
-- columns: policyname
-- row: b7_p1
-- row: b7_p2
-- end-expected
SELECT policyname::text FROM pg_policies WHERE tablename = 'b7_rls' ORDER BY 1;

ALTER POLICY b7_p1 ON b7_rls USING (a > 1);

ALTER POLICY b7_p1 ON b7_rls TO PUBLIC;

-- ============================================================================
-- A function rename names the signature it clashes with
-- ============================================================================

CREATE FUNCTION b7_f(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql;

CREATE FUNCTION b7_g(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42723
-- message-like: function b7_g(integer) already exists in schema "public"
-- end-expected-error
ALTER FUNCTION b7_f(int) RENAME TO b7_g;

-- a different argument list is a different function and does not collide
CREATE FUNCTION b7_g(text) RETURNS text AS $$ SELECT $1 $$ LANGUAGE sql;

ALTER FUNCTION b7_f(int) RENAME TO b7_h;

-- ============================================================================
-- DEPENDS ON EXTENSION names an extension that has to exist
-- ============================================================================

CREATE FUNCTION b7_dep(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: extension "nosuchext" does not exist
-- end-expected-error
ALTER FUNCTION b7_dep(int) DEPENDS ON EXTENSION nosuchext;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: extension "nosuchext" does not exist
-- end-expected-error
ALTER FUNCTION b7_dep(int) NO DEPENDS ON EXTENSION nosuchext;

-- one PostgreSQL ships with counts as installed
ALTER FUNCTION b7_dep(int) DEPENDS ON EXTENSION plpgsql;

-- begin-expected
-- columns: r
-- row: 7
-- end-expected
SELECT b7_dep(7)::text AS r;

-- NO belongs to the attribute when DEPENDS does not follow it
CREATE FUNCTION b7_at(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql;

ALTER FUNCTION b7_at(int) NOT LEAKPROOF;

ALTER FUNCTION b7_at(int) IMMUTABLE;

-- begin-expected
-- columns: v
-- row: i
-- end-expected
SELECT provolatile::text AS v FROM pg_proc WHERE proname = 'b7_at';

-- ============================================================================
-- Teardown
-- ============================================================================

DROP FUNCTION b7_at(int);

DROP FUNCTION b7_dep(int);

DROP FUNCTION b7_g(text);

DROP FUNCTION b7_g(int);

DROP FUNCTION b7_h(int);

DROP POLICY b7_p2 ON b7_rls;

DROP POLICY b7_p1 ON b7_rls;

DROP TABLE b7_rls;

DROP PUBLICATION b7_pub;

DROP TABLE b7_pa;

DROP TABLE b7_pb;

DROP OPERATOR <^ (int, int);

DROP FUNCTION b7_lt(int, int);

DROP TEXT SEARCH CONFIGURATION b7_cfg2;

DROP TEXT SEARCH DICTIONARY b7_dict2;

DROP COLLATION b7_co2;

DROP AGGREGATE b7_ag2(int);

DROP DOMAIN b7_dom;

DROP TYPE b7_ty;

DROP TEXT SEARCH CONFIGURATION b7_cfg;

DROP TEXT SEARCH DICTIONARY b7_dict;

DROP COLLATION b7_co;

DROP TABLE b7_t;

DROP SCHEMA b7 CASCADE;

-- ============================================================================
-- What a value is worth at the edges of its type
-- ============================================================================

-- time runs to 24:00:00 inclusive: the end of the day, not a clock reading
-- begin-expected
-- columns: t
-- row: 24:00:00
-- end-expected
SELECT '24:00:00'::time::text AS t;

-- begin-expected
-- columns: t
-- row: 24:00:00+00
-- end-expected
SELECT '24:00:00'::timetz::text AS t;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('24:00:00'::time > '23:59:59'::time)::text AS r;

-- begin-expected
-- columns: h
-- row: 24
-- end-expected
SELECT extract(hour from '24:00:00'::time)::text AS h;

-- begin-expected
-- columns: t
-- row: 00:00:01
-- end-expected
SELECT ('24:00:00'::time + interval '1 second')::text AS t;

-- begin-expected
-- columns: t
-- row: 23:59:59
-- end-expected
SELECT ('24:00:00'::time - interval '1 second')::text AS t;

-- begin-expected
-- columns: i
-- row: 24:00:00
-- end-expected
SELECT '24:00:00'::time::interval::text AS i;

-- and it is the only hour-24 time the type takes
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "24:00:01"
-- end-expected-error
SELECT '24:00:01'::time;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "24:00:00.000001"
-- end-expected-error
SELECT '24:00:00.000001'::time;

-- a value is rounded to it rather than truncated away from it
-- begin-expected
-- columns: t
-- row: 24:00:00
-- end-expected
SELECT '23:59:59.9999999'::time::text AS t;

-- begin-expected
-- columns: t
-- row: 23:59:59.999999
-- end-expected
SELECT '23:59:59.999999'::time::text AS t;

-- time minus time is an interval, and it may point backwards
-- begin-expected
-- columns: ty
-- row: interval
-- end-expected
SELECT pg_typeof(time '10:00' - time '11:00')::text AS ty;

-- begin-expected
-- columns: i
-- row: -01:00:00
-- end-expected
SELECT (time '10:00' - time '11:00')::text AS i;

-- begin-expected
-- columns: i
-- row: 01:00:00
-- end-expected
SELECT (time '11:00' - time '10:00')::text AS i;

-- a jsonb number is a numeric, so the exponent it was written with is not part of it
-- begin-expected
-- columns: j
-- row: 100
-- end-expected
SELECT '1e2'::jsonb::text AS j;

-- begin-expected
-- columns: j
-- row: 100
-- end-expected
SELECT '1E2'::jsonb::text AS j;

-- begin-expected
-- columns: j
-- row: -100
-- end-expected
SELECT '-1e2'::jsonb::text AS j;

-- begin-expected
-- columns: j
-- row: 1500
-- end-expected
SELECT '1.5e3'::jsonb::text AS j;

-- begin-expected
-- columns: j
-- row: 0.001
-- end-expected
SELECT '1e-3'::jsonb::text AS j;

-- begin-expected
-- columns: j
-- row: [100]
-- end-expected
SELECT '[1e2]'::jsonb::text AS j;

-- begin-expected
-- columns: j
-- row: {"a": 100}
-- end-expected
SELECT '{"a":1e2}'::jsonb::text AS j;

-- json keeps the text as written, which is the whole difference between them
-- begin-expected
-- columns: j
-- row: 1e2
-- end-expected
SELECT '1e2'::json::text AS j;

-- a negative jsonb subscript counts back from the end
-- begin-expected
-- columns: j
-- row: 3
-- end-expected
SELECT '[1,2,3]'::jsonb -> -1 AS j;

-- begin-expected
-- columns: j
-- row: 3
-- end-expected
SELECT '[1,2,3]'::jsonb -> (-1) AS j;

-- begin-expected
-- columns: j
-- row: 1
-- end-expected
SELECT '[1,2,3]'::jsonb -> -3 AS j;

-- begin-expected
-- columns: j
-- row: NULL
-- end-expected
SELECT '[1,2,3]'::jsonb -> -4 AS j;

-- jsonb_set answers with a jsonb even when the path reached nothing to change
-- begin-expected
-- columns: j
-- row: {"a": 1}
-- end-expected
SELECT jsonb_set('{"a":1}', '{b}', '2', false)::text AS j;

-- begin-expected
-- columns: j
-- row: {"a": 2}
-- end-expected
SELECT jsonb_set('{"a":1}', '{a}', '2', false)::text AS j;

-- to_hex is two's complement, and how wide the argument is decides the answer
-- begin-expected
-- columns: h
-- row: ffffffff
-- end-expected
SELECT to_hex((-1)::int4) AS h;

-- begin-expected
-- columns: h
-- row: ffffffffffffffff
-- end-expected
SELECT to_hex((-1)::int8) AS h;

-- there is no bucket to fall into when none were asked for
-- begin-expected-error
-- sqlstate: 2201G
-- message-like: count must be greater than zero
-- end-expected-error
SELECT width_bucket(5.0, 1.0, 10.0, 0);

-- begin-expected-error
-- sqlstate: 2201G
-- message-like: count must be greater than zero
-- end-expected-error
SELECT width_bucket(5.0, 1.0, 10.0, -1);

-- a clock field out of range is the caller's mistake, not an internal fault
-- begin-expected-error
-- sqlstate: 22008
-- message-like: time field value out of range: 25:00:00
-- end-expected-error
SELECT make_time(25, 0, 0);

-- begin-expected-error
-- sqlstate: 22008
-- message-like: time field value out of range: 0:60:00
-- end-expected-error
SELECT make_time(0, 60, 0);

