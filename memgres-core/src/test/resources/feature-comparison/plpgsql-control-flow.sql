-- PL/pgSQL control flow: labelled EXIT and CONTINUE, labelled blocks, and the lifetime of a
-- temp table created ON COMMIT DROP inside a function body.

CREATE OR REPLACE FUNCTION pcf_nested() RETURNS text AS $$
DECLARE i int;
BEGIN
  <<lp>> LOOP
    i := 0;
    LOOP i := i + 1; EXIT lp WHEN i > 1; END LOOP;
  END LOOP;
  RETURN 'done';
END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: a
-- row: done
-- end-expected
SELECT pcf_nested() AS a;

-- A label may be spelled with a word that is also a SQL keyword.
CREATE OR REPLACE FUNCTION pcf_keyword_label() RETURNS text AS $$
DECLARE i int;
BEGIN
  <<outer>> LOOP
    i := 0;
    LOOP i := i + 1; EXIT outer WHEN i > 1; END LOOP;
  END LOOP;
  RETURN 'done';
END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: a
-- row: done
-- end-expected
SELECT pcf_keyword_label() AS a;

CREATE OR REPLACE FUNCTION pcf_keyword_continue() RETURNS text AS $$
DECLARE t text := ''; i int;
BEGIN
  <<outer>> FOR i IN 1..3 LOOP
    CONTINUE outer WHEN i = 2;
    t := t || i::text;
  END LOOP;
  RETURN t;
END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: a
-- row: 13
-- end-expected
SELECT pcf_keyword_continue() AS a;

-- An unlabelled EXIT still leaves only the innermost loop.
CREATE OR REPLACE FUNCTION pcf_unlabelled() RETURNS text AS $$
DECLARE t text := ''; i int;
BEGIN
  FOR i IN 1..2 LOOP
    LOOP t := t || 'a'; EXIT; END LOOP;
    EXIT WHEN true;
  END LOOP;
  RETURN t || 'b';
END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: a
-- row: ab
-- end-expected
SELECT pcf_unlabelled() AS a;

-- EXIT may name a block, and then resumes after that block's END.
CREATE OR REPLACE FUNCTION pcf_block_exit() RETURNS text AS $$
DECLARE t text := '';
BEGIN
  <<blk>> BEGIN
    EXIT blk;
    t := 'inside';
  END;
  RETURN t || 'after';
END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: a
-- row: after
-- end-expected
SELECT pcf_block_exit() AS a;

CREATE OR REPLACE FUNCTION pcf_block_exit_nested() RETURNS text AS $$
DECLARE t text := '';
BEGIN
  <<blk>> BEGIN
    BEGIN EXIT blk; END;
    t := 'inside';
  END;
  RETURN t || 'after';
END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: a
-- row: after
-- end-expected
SELECT pcf_block_exit_nested() AS a;

-- A function body is one statement, so its ON COMMIT DROP table lives until the call returns.
CREATE OR REPLACE FUNCTION pcf_tmp() RETURNS int AS $$
BEGIN
  CREATE TEMP TABLE pcf_t (i int) ON COMMIT DROP;
  INSERT INTO pcf_t VALUES (1),(2);
  RETURN (SELECT count(*) FROM pcf_t);
END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT pcf_tmp() AS a;

-- ...and is gone once that statement is over.
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*) AS a FROM information_schema.tables WHERE table_name = 'pcf_t';

DROP FUNCTION pcf_nested();
DROP FUNCTION pcf_keyword_label();
DROP FUNCTION pcf_keyword_continue();
DROP FUNCTION pcf_unlabelled();
DROP FUNCTION pcf_block_exit();
DROP FUNCTION pcf_block_exit_nested();
DROP FUNCTION pcf_tmp();
