-- What a dropped relation takes with it, and what it leaves standing.
--
-- PostgreSQL records a dependency from a routine to a relation only where it parsed the
-- routine's body when the routine was defined. A SQL-standard body -- BEGIN ATOMIC ... END,
-- or RETURN expr -- is parsed then, so the relations it names are recorded, DROP TABLE is
-- refused while such a routine stands, and CASCADE takes the routine away with the table.
-- The text of a PL/pgSQL body, and of a SQL body written as a string, is resolved when the
-- routine is called: nothing in the catalogue says what it mentions, so the relation can be
-- dropped out from under it, the plain DROP is not refused, and CASCADE leaves the routine
-- exactly where it is -- callable again the moment a relation of that name is back. A
-- routine that answers with the relation's own composite type is a third thing again: it
-- depends on the type rather than on the table, which is what the refusal's DETAIL says.
--
-- The harness keeps only the first line of an error, so the detail-like:/hint-like: lines
-- below are documentation of what both engines send in those fields rather than something
-- it compares.
--
-- Every value below was measured against PostgreSQL 18.

-- ============================================================================
-- A body PostgreSQL never parsed records nothing, so the plain DROP goes through
-- ============================================================================

CREATE TABLE dtk_t1 (id int, k int);

CREATE FUNCTION dtk_pl() RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN RETURN (SELECT count(*) FROM dtk_t1); END $$;

CREATE FUNCTION dtk_sq() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM dtk_t1 $$;

CREATE FUNCTION dtk_tg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;

CREATE TRIGGER dtk_trg BEFORE INSERT ON dtk_t1 FOR EACH ROW EXECUTE FUNCTION dtk_tg();

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname IN ('dtk_pl','dtk_sq','dtk_tg');

DROP TABLE dtk_t1;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname IN ('dtk_pl','dtk_sq','dtk_tg');

-- The trigger goes: it belongs to the relation. The function it named does not.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_trigger WHERE tgname = 'dtk_trg';

DROP FUNCTION dtk_pl();
DROP FUNCTION dtk_sq();
DROP FUNCTION dtk_tg();


-- ============================================================================
-- and CASCADE takes none of them either
-- ============================================================================

CREATE TABLE dtk_t2 (id int, k int);

CREATE FUNCTION dtk_pl2() RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN RETURN (SELECT count(*) FROM dtk_t2); END $$;

CREATE FUNCTION dtk_sq2() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM dtk_t2 $$;

CREATE FUNCTION dtk_tg2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;

CREATE TRIGGER dtk_trg2 BEFORE INSERT ON dtk_t2 FOR EACH ROW EXECUTE FUNCTION dtk_tg2();

DROP TABLE dtk_t2 CASCADE;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname IN ('dtk_pl2','dtk_sq2','dtk_tg2');

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_trigger WHERE tgname = 'dtk_trg2';

DROP FUNCTION dtk_pl2();
DROP FUNCTION dtk_sq2();
DROP FUNCTION dtk_tg2();


-- ============================================================================
-- what CASCADE leaves standing is still the routine that was there
-- ============================================================================

CREATE TABLE dtk_r1 (id int, k int);
INSERT INTO dtk_r1 VALUES (1,10),(2,20);

CREATE FUNCTION dtk_r1pl() RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN RETURN (SELECT count(*) FROM dtk_r1); END $$;

CREATE FUNCTION dtk_r1sq() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM dtk_r1 $$;

CREATE PROCEDURE dtk_r1pr() LANGUAGE plpgsql AS $$
BEGIN DELETE FROM dtk_r1 WHERE id = -1; END $$;

-- begin-expected
-- columns: pl | sq
-- row: 2, 2
-- end-expected
SELECT dtk_r1pl() AS pl, dtk_r1sq() AS sq;

DROP TABLE dtk_r1 CASCADE;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname IN ('dtk_r1pl','dtk_r1sq','dtk_r1pr');

-- It is there, and it says what any routine says about a relation that is not.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dtk_r1" does not exist
-- end-expected-error
SELECT dtk_r1pl();

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dtk_r1" does not exist
-- end-expected-error
SELECT dtk_r1sq();

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dtk_r1" does not exist
-- end-expected-error
CALL dtk_r1pr();

CREATE TABLE dtk_r1 (id int, k int);
INSERT INTO dtk_r1 VALUES (7,70);

-- and answers again the moment a relation of that name is back
-- begin-expected
-- columns: pl | sq
-- row: 1, 1
-- end-expected
SELECT dtk_r1pl() AS pl, dtk_r1sq() AS sq;

CALL dtk_r1pr();

DROP TABLE dtk_r1;
DROP FUNCTION dtk_r1pl();
DROP FUNCTION dtk_r1sq();
DROP PROCEDURE dtk_r1pr();


-- ============================================================================
-- %ROWTYPE and %TYPE in a PL/pgSQL body are resolved when the routine is called,
-- so neither of them is a dependency either
-- ============================================================================

CREATE TABLE dtk_t3 (id int, k int);

CREATE FUNCTION dtk_rw() RETURNS void LANGUAGE plpgsql AS $$
DECLARE r dtk_t3%ROWTYPE; BEGIN r.id := 1; END $$;

CREATE FUNCTION dtk_ty() RETURNS void LANGUAGE plpgsql AS $$
DECLARE v dtk_t3.id%TYPE; BEGIN v := 1; END $$;

DROP TABLE dtk_t3 CASCADE;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname IN ('dtk_rw','dtk_ty');

DROP FUNCTION dtk_rw();
DROP FUNCTION dtk_ty();


-- ============================================================================
-- a function a column default or a CHECK calls is not dropped with the table
-- that calls it
-- ============================================================================

CREATE FUNCTION dtk_df() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 42 $$;

CREATE FUNCTION dtk_ck(int) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT $1 > 0 $$;

CREATE TABLE dtk_t4 (id int DEFAULT dtk_df(), k int CHECK (dtk_ck(k)));

DROP TABLE dtk_t4 CASCADE;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname IN ('dtk_df','dtk_ck');

DROP FUNCTION dtk_df();
DROP FUNCTION dtk_ck(int);


-- ============================================================================
-- a view over the relation is a dependency, and CASCADE does take it
-- ============================================================================

CREATE TABLE dtk_v1 (id int, k int);
CREATE VIEW dtk_v1v AS SELECT id FROM dtk_v1;
CREATE FUNCTION dtk_v1f() RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN RETURN (SELECT count(*) FROM dtk_v1); END $$;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtk_v1 because other objects depend on it
-- detail-like: view dtk_v1v depends on table dtk_v1
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE dtk_v1;

DROP TABLE dtk_v1 CASCADE;

-- begin-expected
-- columns: views
-- row: 0
-- end-expected
SELECT count(*) AS views FROM pg_views WHERE viewname = 'dtk_v1v';

-- the routine beside it is left where it was
-- begin-expected
-- columns: routines
-- row: 1
-- end-expected
SELECT count(*) AS routines FROM pg_proc WHERE proname = 'dtk_v1f';

DROP FUNCTION dtk_v1f();


-- ============================================================================
-- a SQL-standard body is parsed when it is defined, and every relation it names
-- is recorded
-- ============================================================================

CREATE TABLE dtk_t5 (id int, val int);

CREATE FUNCTION dtk_at() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM dtk_t5; END;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtk_t5 because other objects depend on it
-- detail-like: function dtk_at() depends on table dtk_t5
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE dtk_t5;

-- the refusal drops nothing
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'dtk_t5';

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'dtk_at';

DROP TABLE dtk_t5 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'dtk_at';


-- ============================================================================
-- and so is the RETURN form of it
-- ============================================================================

CREATE TABLE dtk_t6 (id int, val int);

CREATE FUNCTION dtk_rr() RETURNS bigint LANGUAGE sql STABLE RETURN (SELECT sum(val) FROM dtk_t6);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtk_t6 because other objects depend on it
-- detail-like: function dtk_rr() depends on table dtk_t6
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE dtk_t6;

DROP TABLE dtk_t6 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'dtk_rr';


-- ============================================================================
-- the relation a body names is the relation it names, not a name its text holds
-- ============================================================================

CREATE TABLE dtk_s1 (id int, val int);
CREATE TABLE dtk_s1x (id int, val int);

-- one SQL-standard body over each of the two, whose names differ by a letter
CREATE FUNCTION dtk_go_at() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM dtk_s1; END;
CREATE FUNCTION dtk_keep_at() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM dtk_s1x; END;

-- a body that only holds the name as a string, and one PostgreSQL never parsed
CREATE FUNCTION dtk_str() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'dtk_s1'::text $$;
CREATE FUNCTION dtk_pls() RETURNS text LANGUAGE plpgsql AS $$
BEGIN RETURN (SELECT count(*)::text FROM dtk_s1); END $$;

-- only the one body that really reads dtk_s1 is named
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtk_s1 because other objects depend on it
-- detail-like: function dtk_go_at() depends on table dtk_s1
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE dtk_s1;

DROP TABLE dtk_s1 CASCADE;

-- begin-expected
-- columns: gone
-- row: 0
-- end-expected
SELECT count(*) AS gone FROM pg_proc WHERE proname = 'dtk_go_at';

-- begin-expected
-- columns: kept
-- row: 3
-- end-expected
SELECT count(*) AS kept FROM pg_proc WHERE proname IN ('dtk_keep_at','dtk_str','dtk_pls');

-- begin-expected
-- columns: still_answers
-- row: 0
-- end-expected
SELECT dtk_keep_at() AS still_answers;

-- begin-expected
-- columns: still_answers
-- row: dtk_s1
-- end-expected
SELECT dtk_str() AS still_answers;

-- and dropping the relation that body really names does take it
DROP TABLE dtk_s1x CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'dtk_keep_at';

DROP FUNCTION dtk_str();
DROP FUNCTION dtk_pls();


-- ============================================================================
-- a routine that answers with the relation's own composite type depends on the
-- type, and the refusal says so
-- ============================================================================

CREATE TABLE dtk_t7 (id int, k int);

CREATE FUNCTION dtk_rt() RETURNS dtk_t7 LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtk_t7 because other objects depend on it
-- detail-like: function dtk_rt() depends on type dtk_t7
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE dtk_t7;

DROP TABLE dtk_t7 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'dtk_rt';


-- ============================================================================
-- the same for SETOF, and the argument list is written out as PostgreSQL writes it
-- ============================================================================

CREATE TABLE dtk_t8 (id int, k int);

CREATE FUNCTION dtk_st() RETURNS SETOF dtk_t8 LANGUAGE sql AS $$ SELECT * FROM dtk_t8 $$;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtk_t8 because other objects depend on it
-- detail-like: function dtk_st() depends on type dtk_t8
-- end-expected-error
DROP TABLE dtk_t8;

DROP TABLE dtk_t8 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'dtk_st';

CREATE TABLE dtk_t9 (id int, val int);

CREATE FUNCTION dtk_ar(n int) RETURNS bigint LANGUAGE sql
BEGIN ATOMIC SELECT count(*) FROM dtk_t9 WHERE id = n; END;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtk_t9 because other objects depend on it
-- detail-like: function dtk_ar(integer) depends on table dtk_t9
-- end-expected-error
DROP TABLE dtk_t9;

DROP TABLE dtk_t9 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'dtk_ar';
