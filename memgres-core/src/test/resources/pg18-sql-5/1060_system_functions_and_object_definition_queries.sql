DROP SCHEMA IF EXISTS test_1060 CASCADE;
CREATE SCHEMA test_1060;
SET search_path TO test_1060;

CREATE TABLE docs (
    doc_id integer PRIMARY KEY,
    title text NOT NULL
);

COMMENT ON TABLE docs IS 'documentation table';
COMMENT ON COLUMN docs.title IS 'title column';

CREATE INDEX docs_title_idx ON docs(title);

CREATE FUNCTION bump(x integer)
RETURNS integer
LANGUAGE SQL
AS $$
  SELECT x + 1
$$;

-- begin-expected
-- columns: table_comment
-- row: documentation table
-- end-expected
SELECT obj_description('test_1060.docs'::regclass) AS table_comment;

-- begin-expected
-- columns: column_comment
-- row: title column
-- end-expected
SELECT col_description('test_1060.docs'::regclass, 2) AS column_comment;

-- begin-expected
-- columns: indexdef_has_title
-- row: t
-- end-expected
SELECT position('title' in pg_get_indexdef('test_1060.docs_title_idx'::regclass)) > 0 AS indexdef_has_title;

-- begin-expected
-- columns: function_exists
-- row: t
-- end-expected
SELECT to_regprocedure('test_1060.bump(integer)') IS NOT NULL AS function_exists;

-- begin-expected
-- columns: has_postgresql_word
-- row: t
-- end-expected
SELECT position('PostgreSQL' in version()) > 0 AS has_postgresql_word;

