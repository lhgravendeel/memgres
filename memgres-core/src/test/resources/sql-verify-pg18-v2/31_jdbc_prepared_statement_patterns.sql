\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE prep_t(
  id int PRIMARY KEY,
  a int,
  b text,
  c timestamptz DEFAULT CURRENT_TIMESTAMP,
  flag boolean
);

INSERT INTO prep_t(id, a, b, flag) VALUES
(1, 10, 'x', true),
(2, NULL, 'y', false),
(3, 30, NULL, NULL);

-- common PreparedStatement patterns
PREPARE p_null AS SELECT $1 IS NULL, pg_typeof($1);
EXECUTE p_null(NULL);

PREPARE p_cast AS SELECT $1::int + 1, pg_typeof($1::int);
EXECUTE p_cast('41');
EXECUTE p_cast(NULL);

PREPARE p_limit(int) AS
  SELECT id, a FROM prep_t ORDER BY id LIMIT $1;
EXECUTE p_limit(2);

PREPARE p_offset(int, int) AS
  SELECT id, a FROM prep_t ORDER BY id LIMIT $1 OFFSET $2;
EXECUTE p_offset(2, 1);

PREPARE p_any(int[]) AS
  SELECT 2 = ANY($1), 4 = ANY($1);
EXECUTE p_any(ARRAY[1,2,3]);

PREPARE p_inferred AS
  SELECT COALESCE($1, 'fallback'), pg_typeof(COALESCE($1, 'fallback'));
EXECUTE p_inferred(NULL);
EXECUTE p_inferred('abc');

PREPARE p_case AS
  SELECT CASE WHEN $1 THEN 'yes' ELSE 'no' END;
EXECUTE p_case(true);
EXECUTE p_case(false);

PREPARE p_values AS
  INSERT INTO prep_t(id, a, b, flag)
  VALUES ($1, $2, $3, $4)
  RETURNING id, a, b, flag;
EXECUTE p_values(10, 100, 'ps', true);

PREPARE p_row AS
  SELECT ROW($1::int, $2::text), pg_typeof(ROW($1::int, $2::text));
EXECUTE p_row(5, 'five');

PREPARE p_array AS
  SELECT array_length($1::int[], 1), pg_typeof($1::int[]);
EXECUTE p_array(ARRAY[1,2,3]);

PREPARE p_order(bool) AS
  SELECT id, a FROM prep_t ORDER BY CASE WHEN $1 THEN id ELSE a END NULLS LAST;
EXECUTE p_order(true);
EXECUTE p_order(false);

PREPARE p_ts AS
  SELECT $1::timestamptz AT TIME ZONE 'UTC';
EXECUTE p_ts('2024-01-01 12:00:00+00');

PREPARE p_like(text) AS
  SELECT id, b FROM prep_t WHERE coalesce(b, '') LIKE $1 ORDER BY id;
EXECUTE p_like('x%');
EXECUTE p_like('%');

-- same statement reused with different values
EXECUTE p_limit(1);
EXECUTE p_limit(3);
EXECUTE p_any(ARRAY[2,4,6]);

-- bad prepared statement cases
PREPARE bad_null AS SELECT $1 + 1;
EXECUTE bad_null(NULL);
PREPARE bad_case AS SELECT CASE WHEN $1 THEN 1 ELSE 'x' END;
EXECUTE bad_case(true);
PREPARE bad_limit(text) AS SELECT * FROM prep_t LIMIT $1;
EXECUTE bad_limit('x');
PREPARE bad_any(text) AS SELECT 1 = ANY($1);
EXECUTE bad_any('abc');
PREPARE bad_row AS SELECT ROW($1).nope;
EXECUTE bad_row(1);
PREPARE bad_count(int) AS SELECT * FROM prep_t WHERE id = $1 AND a = $2;
EXECUTE bad_count(1);
PREPARE bad_offset(int) AS SELECT * FROM prep_t OFFSET $1;
EXECUTE bad_offset(-1);

DEALLOCATE p_null;
DEALLOCATE p_cast;
DEALLOCATE p_limit;
DEALLOCATE p_offset;
DEALLOCATE p_any;
DEALLOCATE p_inferred;
DEALLOCATE p_case;
DEALLOCATE p_values;
DEALLOCATE p_row;
DEALLOCATE p_array;
DEALLOCATE p_order;
DEALLOCATE p_ts;
DEALLOCATE p_like;

DROP SCHEMA compat CASCADE;
