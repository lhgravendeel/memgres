-- ============================================================================
-- Feature Comparison: ruleutils-faithful expression deparsing
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL never echoes the SQL text that was typed for pg_get_constraintdef,
-- pg_get_indexdef or information_schema.check_constraints.check_clause. It
-- deparses the post-parse-analysis tree, so every implicit cast the analyzer
-- inserted is visible and every constant carries a resolved type. Hence
-- CHECK (price >= 0) on numeric prints as CHECK ((price >= (0)::numeric)) while
-- the identical text on an integer column prints unchanged.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS edf_t CASCADE;
DROP TABLE IF EXISTS edf_s CASCADE;
DROP TABLE IF EXISTS edf_n CASCADE;
DROP TABLE IF EXISTS edf_i CASCADE;
DROP DOMAIN IF EXISTS edf_dom CASCADE;

CREATE TABLE edf_t (
  price numeric,
  qty int,
  name text,
  vc varchar(20),
  d date,
  b bool,
  f float8,
  s smallint,
  bi bigint,
  CONSTRAINT edf_num CHECK (price >= 0),
  CONSTRAINT edf_int CHECK (qty > 0),
  CONSTRAINT edf_txt CHECK (name <> ''),
  CONSTRAINT edf_vc CHECK (vc <> 'x'),
  CONSTRAINT edf_and CHECK (qty > 0 AND price < 100),
  CONSTRAINT edf_and3 CHECK (qty = 1 AND qty = 2 AND qty = 3),
  CONSTRAINT edf_notandor CHECK (NOT (qty > 0 AND price < 100)),
  CONSTRAINT edf_arith CHECK (qty + 1 > 2),
  CONSTRAINT edf_arith2 CHECK (price * 2 >= 0),
  CONSTRAINT edf_fn CHECK (lower(name) <> 'zz'),
  CONSTRAINT edf_fnvc CHECK (lower(vc) <> 'zz'),
  CONSTRAINT edf_len CHECK (length(vc) > 0),
  CONSTRAINT edf_between CHECK (qty BETWEEN 1 AND 10),
  CONSTRAINT edf_like CHECK (name LIKE 'a%'),
  CONSTRAINT edf_notlike CHECK (name NOT LIKE 'a%'),
  CONSTRAINT edf_bool CHECK (b),
  CONSTRAINT edf_float CHECK (f > 0),
  CONSTRAINT edf_small CHECK (s > 0),
  CONSTRAINT edf_big CHECK (bi > 0),
  CONSTRAINT edf_date CHECK (d > '2000-01-01'),
  CONSTRAINT edf_neg CHECK (qty > -1),
  CONSTRAINT edf_numlit CHECK (price >= 0.5),
  CONSTRAINT edf_concat CHECK (name || vc <> 'q'),
  CONSTRAINT edf_pow CHECK (qty ^ 2 > 0),
  CONSTRAINT edf_greatest CHECK (greatest(qty, bi) > 0),
  CONSTRAINT edf_in CHECK (qty IN (1,2,3)),
  CONSTRAINT edf_biglit CHECK (qty > 2147483648)
);

-- ============================================================================
-- 1. Constant typing and implicit casts in pg_get_constraintdef
-- ============================================================================

-- begin-expected
-- columns: conname | def
-- row: edf_and, CHECK (((qty > 0) AND (price < (100)::numeric)))
-- row: edf_and3, CHECK (((qty = 1) AND (qty = 2) AND (qty = 3)))
-- row: edf_arith, CHECK (((qty + 1) > 2))
-- row: edf_arith2, CHECK (((price * (2)::numeric) >= (0)::numeric))
-- row: edf_between, CHECK (((qty >= 1) AND (qty <= 10)))
-- row: edf_big, CHECK ((bi > 0))
-- row: edf_biglit, CHECK ((qty > '2147483648'::bigint))
-- row: edf_bool, CHECK (b)
-- row: edf_concat, CHECK (((name [concat] (vc)::text) <> 'q'::text))
-- row: edf_date, CHECK ((d > '2000-01-01'::date))
-- row: edf_float, CHECK ((f > (0)::double precision))
-- row: edf_fn, CHECK ((lower(name) <> 'zz'::text))
-- row: edf_fnvc, CHECK ((lower((vc)::text) <> 'zz'::text))
-- row: edf_greatest, CHECK ((GREATEST((qty)::bigint, bi) > 0))
-- row: edf_in, CHECK ((qty = ANY (ARRAY[1, 2, 3])))
-- row: edf_int, CHECK ((qty > 0))
-- row: edf_len, CHECK ((length((vc)::text) > 0))
-- row: edf_like, CHECK ((name ~~ 'a%'::text))
-- row: edf_neg, CHECK ((qty > '-1'::integer))
-- row: edf_notandor, CHECK ((NOT ((qty > 0) AND (price < (100)::numeric))))
-- row: edf_notlike, CHECK ((name !~~ 'a%'::text))
-- row: edf_num, CHECK ((price >= (0)::numeric))
-- row: edf_numlit, CHECK ((price >= 0.5))
-- row: edf_pow, CHECK ((((qty)::double precision ^ (2)::double precision) > (0)::double precision))
-- row: edf_small, CHECK ((s > 0))
-- row: edf_txt, CHECK ((name <> ''::text))
-- row: edf_vc, CHECK (((vc)::text <> 'x'::text))
-- end-expected
SELECT c.conname, replace(pg_get_constraintdef(c.oid), '||', '[concat]') AS def
FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid
WHERE r.relname = 'edf_t' AND c.contype = 'c'
ORDER BY c.conname;

-- ============================================================================
-- 2. check_clause is the same text without the CHECK (...) wrapper
-- ============================================================================

-- begin-expected
-- columns: constraint_name | check_clause
-- row: edf_num, (price >= (0)::numeric)
-- row: edf_txt, (name <> ''::text)
-- end-expected
SELECT constraint_name, check_clause
FROM information_schema.check_constraints
WHERE constraint_name IN ('edf_num', 'edf_txt')
ORDER BY constraint_name;

-- ============================================================================
-- 3. Numeric-category operator resolution
-- ============================================================================

CREATE TABLE edf_n (
  i2 smallint, i4 int, i8 bigint, n numeric, f4 real, f8 double precision,
  CONSTRAINT edf_n01 CHECK (i2 + i4 > 0),
  CONSTRAINT edf_n02 CHECK (i4 + i8 > 0),
  CONSTRAINT edf_n03 CHECK (i4 + n > 0),
  CONSTRAINT edf_n04 CHECK (i4 + f4 > 0),
  CONSTRAINT edf_n05 CHECK (i4 + f8 > 0),
  CONSTRAINT edf_n06 CHECK (n + f4 > 0),
  CONSTRAINT edf_n07 CHECK (n + f8 > 0),
  CONSTRAINT edf_n08 CHECK (f4 + f8 > 0),
  CONSTRAINT edf_n09 CHECK (i8 + n > 0)
);

-- begin-expected
-- columns: conname | def
-- row: edf_n01, CHECK (((i2 + i4) > 0))
-- row: edf_n02, CHECK (((i4 + i8) > 0))
-- row: edf_n03, CHECK ((((i4)::numeric + n) > (0)::numeric))
-- row: edf_n04, CHECK ((((i4)::double precision + f4) > (0)::double precision))
-- row: edf_n05, CHECK ((((i4)::double precision + f8) > (0)::double precision))
-- row: edf_n06, CHECK ((((n)::double precision + f4) > (0)::double precision))
-- row: edf_n07, CHECK ((((n)::double precision + f8) > (0)::double precision))
-- row: edf_n08, CHECK (((f4 + f8) > (0)::double precision))
-- row: edf_n09, CHECK ((((i8)::numeric + n) > (0)::numeric))
-- end-expected
SELECT c.conname, replace(pg_get_constraintdef(c.oid), '||', '[concat]') AS def
FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid
WHERE r.relname = 'edf_n' AND c.contype = 'c'
ORDER BY c.conname;

-- ============================================================================
-- 4. String-category operator resolution
-- ============================================================================

CREATE TABLE edf_s (
  t text, vc varchar(10), vc2 varchar(5), bc char(5),
  CONSTRAINT edf_s01 CHECK (t = vc),
  CONSTRAINT edf_s02 CHECK (vc = bc),
  CONSTRAINT edf_s03 CHECK (vc = vc2),
  CONSTRAINT edf_s04 CHECK (bc = t),
  CONSTRAINT edf_s05 CHECK (bc <> ''),
  CONSTRAINT edf_s06 CHECK (vc || 'x' <> ''),
  CONSTRAINT edf_s07 CHECK (upper(t) = lower(vc))
);

-- begin-expected
-- columns: conname | def
-- row: edf_s01, CHECK ((t = (vc)::text))
-- row: edf_s02, CHECK (((vc)::bpchar = bc))
-- row: edf_s03, CHECK (((vc)::text = (vc2)::text))
-- row: edf_s04, CHECK (((bc)::text = t))
-- row: edf_s05, CHECK ((bc <> ''::bpchar))
-- row: edf_s06, CHECK ((((vc)::text [concat] 'x'::text) <> ''::text))
-- row: edf_s07, CHECK ((upper(t) = lower((vc)::text)))
-- end-expected
SELECT c.conname, replace(pg_get_constraintdef(c.oid), '||', '[concat]') AS def
FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid
WHERE r.relname = 'edf_s' AND c.contype = 'c'
ORDER BY c.conname;

-- ============================================================================
-- 5. Domain CHECK resolves VALUE against the domain's base type
-- ============================================================================

CREATE DOMAIN edf_dom AS numeric CHECK (VALUE > 0);

-- begin-expected
-- columns: conname | def
-- row: edf_dom_check, CHECK ((VALUE > (0)::numeric))
-- end-expected
SELECT conname, pg_get_constraintdef(oid) AS def
FROM pg_constraint WHERE conname = 'edf_dom_check';

-- ============================================================================
-- 6. Expression indexes and partial-index predicates
-- ============================================================================

CREATE TABLE edf_i (id int, name text, vc varchar(20), price numeric, qty int, c char(5));
CREATE INDEX edf_i1 ON edf_i (lower(name));
CREATE INDEX edf_i2 ON edf_i (lower(vc));
CREATE INDEX edf_i3 ON edf_i ((qty + 1));
CREATE INDEX edf_i4 ON edf_i ((price * 2));
CREATE INDEX edf_i5 ON edf_i (name) WHERE qty > 0;
CREATE INDEX edf_i6 ON edf_i (name) WHERE price > 0;
CREATE INDEX edf_i7 ON edf_i ((name || vc));
CREATE INDEX edf_i8 ON edf_i (upper(c));
CREATE INDEX edf_i9 ON edf_i ((qty::text));
CREATE INDEX edf_i10 ON edf_i ((coalesce(name,'')));

-- begin-expected
-- columns: indexname | indexdef
-- row: edf_i1, CREATE INDEX edf_i1 ON public.edf_i USING btree (lower(name))
-- row: edf_i10, CREATE INDEX edf_i10 ON public.edf_i USING btree (COALESCE(name, ''::text))
-- row: edf_i2, CREATE INDEX edf_i2 ON public.edf_i USING btree (lower((vc)::text))
-- row: edf_i3, CREATE INDEX edf_i3 ON public.edf_i USING btree (((qty + 1)))
-- row: edf_i4, CREATE INDEX edf_i4 ON public.edf_i USING btree (((price * (2)::numeric)))
-- row: edf_i5, CREATE INDEX edf_i5 ON public.edf_i USING btree (name) WHERE (qty > 0)
-- row: edf_i6, CREATE INDEX edf_i6 ON public.edf_i USING btree (name) WHERE (price > (0)::numeric)
-- row: edf_i7, CREATE INDEX edf_i7 ON public.edf_i USING btree (((name [concat] (vc)::text)))
-- row: edf_i8, CREATE INDEX edf_i8 ON public.edf_i USING btree (upper((c)::text))
-- row: edf_i9, CREATE INDEX edf_i9 ON public.edf_i USING btree (((qty)::text))
-- end-expected
SELECT indexname, replace(indexdef, '||', '[concat]') AS indexdef
FROM pg_indexes WHERE tablename = 'edf_i' ORDER BY indexname;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE edf_i CASCADE;
DROP TABLE edf_s CASCADE;
DROP TABLE edf_n CASCADE;
DROP TABLE edf_t CASCADE;
DROP DOMAIN edf_dom CASCADE;
