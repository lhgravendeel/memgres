-- ============================================================================
-- Feature Comparison: Large Object Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests Large Object (LO) functions: lo_creat, lo_create, lo_unlink,
-- lo_from_bytea, lo_get, lo_put, lo_import/export (where testable).
-- LO functions are used by JDBC drivers for LOB handling.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS lo_test CASCADE;
CREATE SCHEMA lo_test;
SET search_path = lo_test, public;

-- ============================================================================
-- 1. lo_creat: create a large object
-- ============================================================================

-- note: lo_creat returns an OID > 0 on success
-- begin-expected
-- columns: is_valid
-- row: true
-- end-expected
SELECT lo_creat(-1) > 0 AS is_valid;

-- ============================================================================
-- 2. lo_create: create with specific OID hint
-- ============================================================================

-- note: lo_create(0) lets PG assign OID; returns the assigned OID
-- begin-expected
-- columns: is_valid
-- row: true
-- end-expected
SELECT lo_create(0) > 0 AS is_valid;

-- ============================================================================
-- 3. lo_from_bytea: create LO from bytea content
-- ============================================================================

-- begin-expected
-- columns: is_valid
-- row: true
-- end-expected
SELECT lo_from_bytea(0, '\x48656c6c6f'::bytea) > 0 AS is_valid;

-- ============================================================================
-- 4. lo_get: retrieve LO content as bytea
-- ============================================================================

-- note: Create an LO, then read it back
CREATE FUNCTION lo_roundtrip() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  content bytea;
BEGIN
  loid := lo_from_bytea(0, 'Hello World'::bytea);
  content := lo_get(loid);
  PERFORM lo_unlink(loid);
  RETURN convert_from(content, 'UTF8');
END;
$$;

-- begin-expected
-- columns: result
-- row: Hello World
-- end-expected
SELECT lo_roundtrip() AS result;

-- ============================================================================
-- 5. lo_put: write data at offset
-- ============================================================================

CREATE FUNCTION lo_put_test() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  content bytea;
BEGIN
  loid := lo_from_bytea(0, 'Hello World'::bytea);
  PERFORM lo_put(loid, 6, 'PG18!'::bytea);
  content := lo_get(loid);
  PERFORM lo_unlink(loid);
  RETURN convert_from(content, 'UTF8');
END;
$$;

-- begin-expected
-- columns: result
-- row: Hello PG18!
-- end-expected
SELECT lo_put_test() AS result;

-- ============================================================================
-- 6. lo_get with offset and length
-- ============================================================================

CREATE FUNCTION lo_get_slice() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  content bytea;
BEGIN
  loid := lo_from_bytea(0, 'Hello World'::bytea);
  content := lo_get(loid, 0, 5);
  PERFORM lo_unlink(loid);
  RETURN convert_from(content, 'UTF8');
END;
$$;

-- begin-expected
-- columns: result
-- row: Hello
-- end-expected
SELECT lo_get_slice() AS result;

-- ============================================================================
-- 7. lo_unlink: delete a large object
-- ============================================================================

CREATE FUNCTION lo_unlink_test() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  result integer;
BEGIN
  loid := lo_creat(-1);
  result := lo_unlink(loid);
  RETURN result;
END;
$$;

-- note: lo_unlink returns 1 on success
-- begin-expected
-- columns: result
-- row: 1
-- end-expected
SELECT lo_unlink_test() AS result;

-- ============================================================================
-- 8. lo_open / lo_close / loread / lowrite (file descriptor API)
-- ============================================================================

-- note: These require transaction context; test basic open/write/read/close cycle

CREATE FUNCTION lo_fd_test() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  fd integer;
  data bytea;
BEGIN
  loid := lo_creat(-1);

  -- Open for writing (INV_WRITE = 0x20000)
  fd := lo_open(loid, x'20000'::integer);
  PERFORM lowrite(fd, 'Test data'::bytea);
  PERFORM lo_close(fd);

  -- Open for reading (INV_READ = 0x40000)
  fd := lo_open(loid, x'40000'::integer);
  data := loread(fd, 9);
  PERFORM lo_close(fd);

  PERFORM lo_unlink(loid);
  RETURN convert_from(data, 'UTF8');
END;
$$;

-- begin-expected
-- columns: result
-- row: Test data
-- end-expected
SELECT lo_fd_test() AS result;

-- ============================================================================
-- 9. lo_lseek / lo_tell: seek and tell position
-- ============================================================================

CREATE FUNCTION lo_seek_test() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  fd integer;
  data bytea;
  pos integer;
BEGIN
  loid := lo_from_bytea(0, 'ABCDEFGHIJ'::bytea);

  fd := lo_open(loid, x'40000'::integer);
  -- Seek to position 5
  PERFORM lo_lseek(fd, 5, 0);  -- 0 = SEEK_SET
  pos := lo_tell(fd);
  data := loread(fd, 3);
  PERFORM lo_close(fd);

  PERFORM lo_unlink(loid);
  RETURN pos::text || ':' || convert_from(data, 'UTF8');
END;
$$;

-- begin-expected
-- columns: result
-- row: 5:FGH
-- end-expected
SELECT lo_seek_test() AS result;

-- ============================================================================
-- 10. lo_truncate: truncate LO to specified length
-- ============================================================================

CREATE FUNCTION lo_truncate_test() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  fd integer;
  content bytea;
BEGIN
  loid := lo_from_bytea(0, 'Hello World!'::bytea);

  fd := lo_open(loid, x'60000'::integer);  -- INV_READ | INV_WRITE
  PERFORM lo_truncate(fd, 5);
  PERFORM lo_close(fd);

  content := lo_get(loid);
  PERFORM lo_unlink(loid);
  RETURN convert_from(content, 'UTF8');
END;
$$;

-- begin-expected
-- columns: result
-- row: Hello
-- end-expected
SELECT lo_truncate_test() AS result;

-- ============================================================================
-- 11. Large object in pg_largeobject_metadata
-- ============================================================================

CREATE FUNCTION lo_metadata_test() RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  loid oid;
  cnt integer;
BEGIN
  loid := lo_creat(-1);
  SELECT count(*) INTO cnt FROM pg_largeobject_metadata WHERE oid = loid;
  PERFORM lo_unlink(loid);
  RETURN cnt > 0;
END;
$$;

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT lo_metadata_test() AS result;

-- ============================================================================
-- 12. Multiple LOs in sequence
-- ============================================================================

CREATE FUNCTION lo_multi_test() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  ids oid[];
  i integer;
  loid oid;
BEGIN
  FOR i IN 1..5 LOOP
    loid := lo_from_bytea(0, ('Data ' || i)::bytea);
    ids := array_append(ids, loid);
  END LOOP;

  -- Clean up
  FOREACH loid IN ARRAY ids LOOP
    PERFORM lo_unlink(loid);
  END LOOP;

  RETURN array_length(ids, 1);
END;
$$;

-- begin-expected
-- columns: result
-- row: 5
-- end-expected
SELECT lo_multi_test() AS result;

-- ============================================================================
-- 13. lo_unlink on nonexistent OID
-- ============================================================================

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT lo_unlink(999999999);

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA lo_test CASCADE;
SET search_path = public;
