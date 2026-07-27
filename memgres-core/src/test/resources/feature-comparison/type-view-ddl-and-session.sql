-- Object definitions and session settings PG 18 accepts: a view that refers to itself, a range
-- type, renaming a composite type, reading the time zone back, and calling a function that
-- supplies all of its own arguments.

DROP VIEW IF EXISTS tvs_v CASCADE;
CREATE RECURSIVE VIEW tvs_v (n) AS SELECT 1 UNION ALL SELECT n+1 FROM tvs_v WHERE n < 3;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM tvs_v;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT max(n) AS n FROM tvs_v;

-- A recursive view is an ordinary view once created.
-- begin-expected
-- columns: k
-- row: v
-- end-expected
SELECT relkind::text AS k FROM pg_class WHERE relname = 'tvs_v';

DROP VIEW tvs_v;

-- A range type is a type like any other.
DROP TYPE IF EXISTS tvs_r CASCADE;
CREATE TYPE tvs_r AS RANGE (subtype = int8);

-- begin-expected
-- columns: t
-- row: tvs_r
-- end-expected
SELECT 'tvs_r'::regtype::text AS t;

-- begin-expected
-- columns: t
-- row: r
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'tvs_r';

DROP TYPE tvs_r;

-- A composite type can be renamed.
DROP TYPE IF EXISTS tvs_c CASCADE;
DROP TYPE IF EXISTS tvs_c2 CASCADE;
CREATE TYPE tvs_c AS (a int);
ALTER TYPE tvs_c RENAME TO tvs_c2;

-- begin-expected
-- columns: t
-- row: tvs_c2
-- end-expected
SELECT typname AS t FROM pg_type WHERE typname = 'tvs_c2';

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'tvs_c';

DROP TYPE tvs_c2;

-- An enum renames the same way.
DROP TYPE IF EXISTS tvs_e CASCADE;
DROP TYPE IF EXISTS tvs_e2 CASCADE;
CREATE TYPE tvs_e AS ENUM ('a','b');
ALTER TYPE tvs_e RENAME TO tvs_e2;

-- begin-expected
-- columns: v
-- row: a
-- end-expected
SELECT 'a'::tvs_e2::text AS v;

DROP TYPE tvs_e2;

-- Attribute-level changes still work.
DROP TYPE IF EXISTS tvs_c3 CASCADE;
CREATE TYPE tvs_c3 AS (a int);
ALTER TYPE tvs_c3 ADD ATTRIBUTE b text;
ALTER TYPE tvs_c3 RENAME ATTRIBUTE b TO c;
ALTER TYPE tvs_c3 DROP ATTRIBUTE c;

-- begin-expected
-- columns: t
-- row: tvs_c3
-- end-expected
SELECT typname AS t FROM pg_type WHERE typname = 'tvs_c3';

DROP TYPE tvs_c3;

-- The time zone reads back under the name it is set with.
SET TIME ZONE 'UTC';

-- begin-expected
-- columns: TimeZone
-- row: UTC
-- end-expected
SHOW TIME ZONE;

-- begin-expected
-- columns: TimeZone
-- row: UTC
-- end-expected
SHOW timezone;

SET TIME ZONE 'Europe/Amsterdam';

-- begin-expected
-- columns: TimeZone
-- row: Europe/Amsterdam
-- end-expected
SHOW TIME ZONE;

RESET TIME ZONE;

-- An unknown setting is still an error.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter
-- end-expected-error
SHOW no_such_setting_at_all;

-- A function may supply every one of its own arguments.
CREATE OR REPLACE FUNCTION tvs_f(a int DEFAULT 1) RETURNS int LANGUAGE sql AS $$ SELECT a $$;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT tvs_f() AS v;

-- begin-expected
-- columns: v
-- row: 5
-- end-expected
SELECT tvs_f(5) AS v;

DROP FUNCTION tvs_f(int);

CREATE OR REPLACE FUNCTION tvs_g(a int, b int DEFAULT 10) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;

-- begin-expected
-- columns: v
-- row: 11
-- end-expected
SELECT tvs_g(1) AS v;

-- begin-expected
-- columns: v
-- row: 3
-- end-expected
SELECT tvs_g(1,2) AS v;

DROP FUNCTION tvs_g(int,int);

-- A variadic parameter with a default may be left out entirely.
CREATE OR REPLACE FUNCTION tvs_h(a int DEFAULT 1, VARIADIC b int[] DEFAULT '{}')
  RETURNS int LANGUAGE sql AS $$ SELECT a $$;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT tvs_h() AS v;

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT tvs_h(2) AS v;

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT tvs_h(2,3,4) AS v;

DROP FUNCTION tvs_h(int,int[]);
