-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Types, casts and coercion
-- title: Unrelated singletons
-- begin-expected
-- columns: ?column?:int4
-- row: 42
-- rowcount: 1
-- end-expected
SELECT 0x2a;
-- begin-expected
-- columns: ?column?:int4
-- row: 10
-- rowcount: 1
-- end-expected
SELECT 0b1010;
-- begin-expected
-- columns: numeric:numeric
-- row: 1000.5
-- rowcount: 1
-- end-expected
SELECT '1_000.5'::numeric;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid hexadecimal integer at or near "0x"
-- end-expected-error
SELECT 0x;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric: "+NaN"
-- end-expected-error
SELECT '+NaN'::numeric;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type uuid: " a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 "
-- end-expected-error
SELECT ' a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 '::uuid;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type uuid: "a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11"
-- end-expected-error
SELECT 'a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
-- begin-expected-error
-- sqlstate: 2201G
-- message-like: lower and upper bounds must be finite
-- end-expected-error
SELECT width_bucket(1::numeric, 0::numeric, 'Infinity'::numeric, 3);
-- begin-expected
-- columns: length:int4
-- row: 16385
-- rowcount: 1
-- end-expected
SELECT length(round(1.5::numeric, 2147483647)::text);
-- begin-expected
-- columns: round:numeric
-- row: 0
-- rowcount: 1
-- end-expected
SELECT round(1.5::numeric, -2147483648);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 2::float8 ^ 10000::float8;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: underflow
-- end-expected-error
SELECT 1e-46::float8::real;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: "1e400" is out of range for type real
-- end-expected-error
SELECT '1e400'::float(24);
-- begin-expected
-- columns: length:int4
-- row: 100001
-- rowcount: 1
-- end-expected
SELECT length((10::numeric ^ 100000::numeric)::text);
-- begin-expected
-- columns: numeric:numeric
-- row: 0.1
-- rowcount: 1
-- end-expected
SELECT 0.1::real::numeric;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 0.1::real::numeric = 0.1::numeric;
-- begin-expected
-- ok: 0
-- end-expected
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: text:text
-- row: 2001-02-16 15:38:40-05
-- rowcount: 1
-- end-expected
SELECT (timestamptz '2001-02-16 20:38:40+00')::text;
-- begin-expected
-- columns: timezone:text
-- row: 2001-02-16 15:38:40
-- rowcount: 1
-- end-expected
SELECT (timestamptz '2001-02-16 20:38:40+00' AT TIME ZONE '+05')::text;
-- begin-expected
-- columns: time:time
-- row: 24:00:00
-- rowcount: 1
-- end-expected
SELECT '23:59:59.9'::time(0);
-- begin-expected-error
-- sqlstate: 22009
-- message-like: time zone displacement out of range: "-4713-01-01 BC"
-- end-expected-error
SELECT '-4713-01-01 BC'::timestamp;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_rc (c regclass);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_rc VALUES ('pg_class'::regclass);
-- begin-expected
-- columns: c:regclass | pg_typeof:regtype
-- row: pg_class | regclass
-- rowcount: 1
-- end-expected
SELECT c, pg_typeof(c) FROM zz_rc;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_v (b numeric);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vv AS SELECT b FROM zz_v WHERE b > 0;
-- begin-expected
-- columns: pg_get_viewdef:text
-- row:  SELECT b\n   FROM zz_v\n  WHERE b > 0::numeric;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vv'::regclass, true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_ds;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dt (a int DEFAULT nextval('zz_ds'));
-- begin-expected
-- columns: column_default:varchar
-- row: nextval('zz_ds'::regclass)
-- rowcount: 1
-- end-expected
SELECT column_default FROM information_schema.columns WHERE table_name='zz_dt' AND column_name='a';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ser (a serial8, b serial4, c serial2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_sa AS integer;
-- begin-expected
-- columns: data_type:regtype
-- row: integer
-- rowcount: 1
-- end-expected
SELECT data_type FROM pg_sequences WHERE sequencename='zz_sa';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_wt3 (f timestamptz);
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in index expression must be marked IMMUTABLE
-- end-expected-error
CREATE INDEX zz_wi ON zz_wt3 ((f::date));
-- begin-expected
-- columns: acldefault:text
-- row: {=arwdDxtm/0}
-- rowcount: 1
-- end-expected
SELECT acldefault('r', 0)::text;
-- begin-expected
-- columns: makeaclitem:aclitem
-- row: =r*/0
-- rowcount: 1
-- end-expected
SELECT makeaclitem(0,0,'SELECT',true);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: unrecognized key word: "garbage"
-- end-expected-error
SELECT 'garbage'::aclitem::text;
-- begin-expected
-- columns: length:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT length(to_tsvector('english', repeat('k',2047))::text);
-- begin-expected-error
-- sqlstate: 54000
-- message-like: word is too long (5001 bytes, max 2046 bytes)
-- end-expected-error
SELECT length(('a'||repeat('b',5000))::tsvector::text);
-- begin-expected
-- columns: ?column?:float8
-- row: 1.4142135623730951e+308
-- rowcount: 1
-- end-expected
SELECT '(0,0)'::point <-> '(1e308,1e308)'::point;
-- begin-expected
-- columns: ?column?:float8
-- row: 4.242640687119286
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box <-> '(3,3),(4,4)'::box;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f3() RETURNS text LANGUAGE sql AS $$ SELECT 'x'::pg_catalog.text $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_assertint() RETURNS void AS $$ BEGIN ASSERT 1; END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_assertint:void
-- row: 
-- rowcount: 1
-- end-expected
SELECT zz_assertint();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_add(a int, b int) RETURNS int AS $$ SELECT COALESCE(a,0)+COALESCE(b,0) $$ LANGUAGE sql;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "not a number"
-- end-expected-error
CREATE AGGREGATE zz_agg (int) (SFUNC = zz_add, STYPE = int, INITCOND = 'not a number');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer + boolean
-- end-expected-error
SELECT 1 + true;
-- Hint
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
SELECT lower(1);
-- Hint
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT 2147483647 + 1;
-- Datatype
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_dom_pos AS int CHECK (VALUE > 0);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zz_dom_pos violates check constraint "zz_dom_pos_check"
-- end-expected-error
SELECT (-1)::zz_dom_pos;
-- Schema / Constraint / Datatype
-- extended protocol, resultFormats = [1]
-- begin-expected
-- columns: numeric:numeric
-- row: 10000000000
-- rowcount: 1
-- end-expected
SELECT 1e10::numeric;
