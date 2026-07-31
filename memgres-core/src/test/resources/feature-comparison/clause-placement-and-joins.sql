-- Where a construct may stand, and what a join or a LATERAL exposes.
--
-- Four things measured against PostgreSQL 18:
--
--  1. A WINDOW CALL IN A CLAUSE THAT CANNOT HOLD ONE. A window function is numbered against the
--     result rows, so a clause read before those rows exist -- WHERE, GROUP BY, HAVING, a join
--     condition, an aggregate's argument, LIMIT, a CHECK, an index predicate, a DEFAULT -- cannot
--     hold one at all, and PostgreSQL names the clause (42P20). The select list and ORDER BY are
--     where one may stand. Two orderings matter and are pinned here: an expression is analysed
--     from the leaves up, so the window call inside an aggregate is what a clause is refused for
--     rather than the aggregate around it; and a window call's OVER specification is transformed
--     with the query's other window definitions, after every clause has been read, so a column
--     that is not there in one is never reached.
--
--  2. WHAT A LATERAL EXPOSES. A column projected through a LATERAL is the column it came from,
--     down to the parts a bare type name does not carry. Typed as text instead, abs() over one
--     answered "function abs(text) does not exist" and an int[] called itself _int4.
--
--  3. AN ORDER BY AMONG A CALL'S ARGUMENTS belongs to an aggregate: it says which order the call
--     accumulates its input in, and a plain function accumulates nothing. PostgreSQL refuses it in
--     the same words it refuses FILTER and DISTINCT, and reads the three in a fixed order --
--     DISTINCT, then ORDER BY, then FILTER -- so a call carrying two is refused for the first.
--
--  4. A JOIN CONDITION HAS TO BE A CONDITION. ON a.id joins on nothing: PostgreSQL coerces the
--     qualification to boolean while it builds the range table, so a text column there is 42804
--     and an unadorned string that is not a boolean word is 22P02.

-- setup
DROP TABLE IF EXISTS cpj_chk CASCADE;
DROP TABLE IF EXISTS cpj_typ CASCADE;
DROP TABLE IF EXISTS cpj_u CASCADE;
DROP TABLE IF EXISTS cpj_t CASCADE;
DROP DOMAIN IF EXISTS cpj_yn CASCADE;

CREATE TABLE cpj_t (v int PRIMARY KEY, s text, flag boolean);
INSERT INTO cpj_t VALUES (1, 'a', true), (2, 'b', false);

CREATE TABLE cpj_u (v int PRIMARY KEY, s text);
INSERT INTO cpj_u VALUES (1, 'x'), (3, 'y');

CREATE DOMAIN cpj_yn AS boolean;

CREATE TABLE cpj_typ (id int PRIMARY KEY, i2 smallint, i8 bigint, n numeric(10,2), f8 double precision,
                      t text, vc varchar(10), c char(3), b boolean, d date, ts timestamp,
                      u uuid, j json, jb jsonb, ba bytea, ar int[], y cpj_yn);
INSERT INTO cpj_typ VALUES (1, 2, 4, 5.50, 7.5, 'txt', 'vc', 'ccc', true,
                           date '2020-01-01', timestamp '2020-01-01 10:00',
                           '11111111-1111-1111-1111-111111111111'::uuid,
                           '{"a":1}'::json, '{"a":1}'::jsonb, '\x0102'::bytea, ARRAY[1,2], true);

-- 1: a window call in a clause that cannot hold one

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT v FROM cpj_t WHERE row_number() OVER (ORDER BY v) = 1;

-- The OVER specification is not read while WHERE is being judged, so the column that is not there
-- in it never comes up.
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT v FROM cpj_t WHERE row_number() OVER (ORDER BY nosuchcol) = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT v FROM cpj_t WHERE row_number() OVER (PARTITION BY nosuchcol) = 1;

-- The call's own arguments ARE read first, so a column that is not there in one is the fault.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT v FROM cpj_t WHERE lag(nosuchcol) OVER () = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT v FROM cpj_t WHERE v = 1 AND rank() OVER () = 1;

-- An expression is analysed from the leaves up, so the window call inside the aggregate is what
-- WHERE is refused for -- not the aggregate that WHERE equally cannot hold.
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT count(*) FROM cpj_t WHERE sum(row_number() OVER ()) > 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in GROUP BY
-- end-expected-error
SELECT count(*) FROM cpj_t GROUP BY row_number() OVER ();

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in HAVING
-- end-expected-error
SELECT count(*) FROM cpj_t HAVING row_number() OVER () = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in HAVING
-- end-expected-error
SELECT count(*) FROM cpj_t HAVING sum(row_number() OVER ()) > 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in JOIN conditions
-- end-expected-error
SELECT cpj_t.v FROM cpj_t JOIN cpj_u ON row_number() OVER () = cpj_u.v;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in JOIN conditions
-- end-expected-error
SELECT cpj_t.v FROM cpj_t LEFT JOIN cpj_u ON cpj_t.v = cpj_u.v AND rank() OVER () = 1;

-- An aggregate cannot read a window call: the window runs over the rows the aggregate produced.
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot contain window function calls
-- end-expected-error
SELECT sum(row_number() OVER ()) FROM cpj_t;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in LIMIT
-- end-expected-error
SELECT v FROM cpj_t LIMIT row_number() OVER ();

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in OFFSET
-- end-expected-error
SELECT v FROM cpj_t OFFSET row_number() OVER ();

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT sum(v) OVER (PARTITION BY row_number() OVER ()) FROM cpj_t;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE cpj_chk (i int CHECK (row_number() OVER () = 1));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in index predicates
-- end-expected-error
CREATE INDEX cpj_ix ON cpj_t (v) WHERE row_number() OVER () = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in index expressions
-- end-expected-error
CREATE INDEX cpj_ix ON cpj_t ((row_number() OVER ()));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in DEFAULT expressions
-- end-expected-error
CREATE TABLE cpj_chk (i int DEFAULT (row_number() OVER ()));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in VALUES
-- end-expected-error
INSERT INTO cpj_t VALUES (row_number() OVER (), 'z', true);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in UPDATE
-- end-expected-error
UPDATE cpj_t SET s = row_number() OVER ()::text;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
DELETE FROM cpj_t WHERE row_number() OVER () = 1;

-- RETURNING looks like a select list but is projected one written row at a time, so it is a
-- clause that cannot hold a window call either.
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in RETURNING
-- end-expected-error
INSERT INTO cpj_t VALUES (9, 'z', true) RETURNING row_number() OVER ();

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in RETURNING
-- end-expected-error
DELETE FROM cpj_t WHERE v = 9 RETURNING row_number() OVER ();

-- 2: where a window call may stand

-- begin-expected
-- columns: v | row_number
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT v, row_number() OVER (ORDER BY v) FROM cpj_t ORDER BY v;

-- begin-expected
-- columns: v
-- row: 2
-- row: 1
-- end-expected
SELECT v FROM cpj_t ORDER BY row_number() OVER (ORDER BY v DESC);

-- begin-expected
-- columns: v | r
-- row: 1, 1
-- end-expected
SELECT * FROM (SELECT v, row_number() OVER (ORDER BY v) r FROM cpj_t) x WHERE r = 1;

-- begin-expected
-- columns: v | r
-- row: 1, 1
-- end-expected
WITH w AS (SELECT v, row_number() OVER (ORDER BY v) r FROM cpj_t) SELECT * FROM w WHERE r = 1;

-- An aggregate in a window's own specification is ordinary: the specification is read once per
-- result row, and by then the groups exist.
-- begin-expected
-- columns: s
-- row: 2
-- row: 2
-- end-expected
SELECT sum(count(*)) OVER (PARTITION BY count(*)) AS s FROM cpj_t GROUP BY v ORDER BY 1;

-- 3: what a LATERAL exposes

-- begin-expected
-- columns: pg_typeof | pg_typeof | pg_typeof | pg_typeof | pg_typeof
-- row: smallint, bigint, numeric, double precision, text
-- end-expected
SELECT pg_typeof(z1), pg_typeof(z2), pg_typeof(z3), pg_typeof(z4), pg_typeof(z5)
FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.i2 AS z1, t.i8 AS z2, t.n AS z3, t.f8 AS z4, t.t AS z5) l;

-- begin-expected
-- columns: pg_typeof | pg_typeof | pg_typeof | pg_typeof
-- row: character varying, character, boolean, date
-- end-expected
SELECT pg_typeof(z1), pg_typeof(z2), pg_typeof(z3), pg_typeof(z4)
FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.vc AS z1, t.c AS z2, t.b AS z3, t.d AS z4) l;

-- begin-expected
-- columns: pg_typeof | pg_typeof | pg_typeof | pg_typeof
-- row: timestamp without time zone, uuid, json, jsonb
-- end-expected
SELECT pg_typeof(z1), pg_typeof(z2), pg_typeof(z3), pg_typeof(z4)
FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.ts AS z1, t.u AS z2, t.j AS z3, t.jb AS z4) l;

-- begin-expected
-- columns: pg_typeof | pg_typeof
-- row: bytea, integer[]
-- end-expected
SELECT pg_typeof(z1), pg_typeof(z2)
FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.ba AS z1, t.ar AS z2) l;

-- An alias list renames what the item exposes, and renaming is all it does.
-- begin-expected
-- columns: pg_typeof | pg_typeof | pg_typeof
-- row: integer, character varying, integer[]
-- end-expected
SELECT pg_typeof(p), pg_typeof(q), pg_typeof(r)
FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.id, t.vc, t.ar) l(p, q, r);

-- The comma form is the same join, so it exposes the same thing.
-- begin-expected
-- columns: pg_typeof | pg_typeof
-- row: integer, integer[]
-- end-expected
SELECT pg_typeof(p), pg_typeof(q) FROM cpj_typ t, LATERAL (SELECT t.id AS p, t.ar AS q) l;

-- begin-expected
-- columns: pg_typeof
-- row: integer
-- end-expected
SELECT pg_typeof(z) FROM cpj_typ t CROSS JOIN LATERAL (WITH w AS (SELECT t.id AS y) SELECT y AS z FROM w) l;

-- An uncorrelated LATERAL and a plain derived table expose what they project.
-- begin-expected
-- columns: pg_typeof
-- row: integer
-- end-expected
SELECT pg_typeof(z) FROM cpj_typ t CROSS JOIN LATERAL (SELECT id AS z FROM cpj_typ) l;

-- A set-returning function in FROM exposes the type the call produces.
-- begin-expected
-- columns: pg_typeof
-- row: integer
-- end-expected
SELECT DISTINCT pg_typeof(g) FROM cpj_typ t CROSS JOIN LATERAL generate_series(1, t.id) AS g;

-- begin-expected
-- columns: pg_typeof
-- row: integer
-- end-expected
SELECT DISTINCT pg_typeof(g) FROM cpj_typ t CROSS JOIN LATERAL unnest(t.ar) AS g;

-- The type a LATERAL exposes is what resolves a call over it, so this is the FILTER refusal and
-- not a complaint about abs over text.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(z) FILTER (WHERE true) FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.id AS z) l;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(z) FILTER (WHERE true) FROM cpj_typ t, LATERAL (SELECT t.id AS z) l;

-- begin-expected
-- columns: abs
-- row: 1
-- end-expected
SELECT abs(z) FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.id AS z) l;

-- begin-expected
-- columns: p
-- row: 3
-- end-expected
SELECT l.p FROM cpj_typ t, LATERAL (SELECT t.id + 2) l(p);

-- 4: an ORDER BY among a plain call's arguments

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v ORDER BY v) FROM cpj_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but length is not an aggregate function
-- end-expected-error
SELECT length(s ORDER BY v) FROM cpj_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but upper is not an aggregate function
-- end-expected-error
SELECT upper('a' ORDER BY v) FROM cpj_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but pg_catalog.abs is not an aggregate function
-- end-expected-error
SELECT pg_catalog.abs(v ORDER BY v) FROM cpj_t;

-- The ORDER BY expressions are not resolved: the call is refused before they are reached.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v ORDER BY nosuchcol) FROM cpj_t;

-- The call's own arguments are resolved first, as they are for FILTER.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT abs(nosuchcol ORDER BY v) FROM cpj_t;

-- The refusal is a property of the call, so it holds wherever the call stands.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
SELECT v FROM cpj_t WHERE abs(v ORDER BY v) = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
UPDATE cpj_t SET s = abs(v ORDER BY v)::text;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
INSERT INTO cpj_t VALUES (abs(3 ORDER BY 1), 'z', true);

-- DISTINCT is read before ORDER BY, and ORDER BY before FILTER.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: DISTINCT specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(DISTINCT v ORDER BY v) FROM cpj_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v ORDER BY v) FILTER (WHERE true) FROM cpj_t;

-- A name that is no function at all is a missing function first.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function cpj_nofunc(integer) does not exist
-- end-expected-error
SELECT cpj_nofunc(v ORDER BY v) FROM cpj_t;

-- An ORDER BY inside an aggregate is what the clause is for.
-- begin-expected
-- columns: string_agg
-- row: b,a
-- end-expected
SELECT string_agg(s, ',' ORDER BY v DESC) FROM cpj_t;

-- begin-expected
-- columns: array_agg
-- row: {2,1}
-- end-expected
SELECT array_agg(v ORDER BY v DESC) FROM cpj_t;

-- begin-expected
-- columns: percentile_cont
-- row: 1.5
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM cpj_t;

-- 5: a join condition has to be a condition

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "x"
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 'x';

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type text
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.s;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type text
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v::text;

-- A call whose every signature returns the same type is as certain as a column is.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type timestamp with time zone
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON now();

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON length(a.s);

-- An outer join is refused for the same reason, before anything it cannot join on comes up.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM cpj_t a FULL JOIN cpj_u b ON a.v;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type text
-- end-expected-error
SELECT count(*) FROM cpj_t a LEFT JOIN cpj_u b ON a.s;

-- AND and OR each want a condition of their own, and PostgreSQL names the operator.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "x"
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v AND 'x';

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of AND must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v AND b.v;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of OR must be type boolean, not type text
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v OR b.s;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of NOT must be type boolean, not type text
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON NOT a.s;

-- A call that cannot stand in a join condition at all is refused for that, whatever it would
-- have been typed.
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in JOIN conditions
-- end-expected-error
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON count(*);

-- 6: the join conditions PostgreSQL runs

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_t b ON a.flag;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_t b ON a.flag AND b.flag;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_t b ON NOT a.flag;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_t b ON a.flag OR b.flag;

-- A domain over boolean is a boolean.
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cpj_typ a JOIN cpj_typ b ON a.y;

-- An unadorned string is read as boolean input, so a boolean word is a condition.
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 't';

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 'off';

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON true;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON NULL;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v AND a.s <> b.s;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON CASE WHEN a.v = 1 THEN true ELSE false END;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON isfinite(now());

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON starts_with(a.s || 'z', a.s);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON coalesce(a.v = b.v, true);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v IN (SELECT v FROM cpj_u);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v JOIN cpj_t c ON c.v = a.v;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM cpj_t a JOIN LATERAL (SELECT a.v AS w) l ON l.w = a.v;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cpj_t a JOIN cpj_u b USING (v);

-- A NATURAL join joins on every name both sides share, which here is s as well as v.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM cpj_t NATURAL JOIN cpj_u;

-- cleanup
DROP TABLE IF EXISTS cpj_chk CASCADE;
DROP TABLE IF EXISTS cpj_typ CASCADE;
DROP TABLE IF EXISTS cpj_u CASCADE;
DROP TABLE IF EXISTS cpj_t CASCADE;
DROP DOMAIN IF EXISTS cpj_yn CASCADE;
