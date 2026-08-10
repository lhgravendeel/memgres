-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ERROR: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_sl1 SCROLL BINARY CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ERROR: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_sl2 INSENSITIVE BINARY CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ERROR: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_sl3 BINARY BINARY CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ERROR: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_sl4 SCROLL INSENSITIVE CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ERROR: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_sl5 NO SCROLL BINARY CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ERROR: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_sl6 SCROLL SCROLL CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42P11
-- message-like: ERROR: cannot specify both SCROLL and NO SCROLL
-- end-expected-error
DECLARE zz_sl7 SCROLL NO SCROLL CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42P11
-- message-like: ERROR: cannot specify both SCROLL and NO SCROLL
-- end-expected-error
DECLARE zz_sl8 NO SCROLL SCROLL CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "FOR"
-- end-expected-error
DECLARE zz_sl9 CURSOR WITH FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "FOR"
-- end-expected-error
DECLARE zz_sla CURSOR WITHOUT FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "CURSOR"
-- end-expected-error
DECLARE zz_slb NO CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "select"
-- end-expected-error
DECLARE select CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "all"
-- end-expected-error
DECLARE all CURSOR FOR SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "3000000000"
-- end-expected-error
FETCH 3000000000 FROM zz_slc;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "3000000000"
-- end-expected-error
FETCH FORWARD 3000000000 FROM zz_slc;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "4000000000"
-- end-expected-error
FETCH ABSOLUTE 4000000000 FROM zz_slc;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "4000000000"
-- end-expected-error
MOVE ABSOLUTE 4000000000 IN zz_slc;

-- begin-expected-error
-- sqlstate: 34000
-- message-like: ERROR: cursor "zz_slc" does not exist
-- end-expected-error
FETCH FORWARD +1 FROM zz_slc;

-- begin-expected-error
-- sqlstate: 34000
-- message-like: ERROR: cursor "zz_slc" does not exist
-- end-expected-error
FETCH FORWARD -1 FROM zz_slc;

-- begin-expected-error
-- sqlstate: 34000
-- message-like: ERROR: cursor "zz_slc" does not exist
-- end-expected-error
FETCH +2 FROM zz_slc;

-- begin-expected-error
-- sqlstate: 34000
-- message-like: ERROR: cursor "zz_slc" does not exist
-- end-expected-error
MOVE FORWARD +1 IN zz_slc;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "junk"
-- end-expected-error
FETCH NEXT FROM zz_slc junk;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "1"
-- end-expected-error
MOVE NEXT FROM zz_slc 1 2 3;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "junk"
-- end-expected-error
CLOSE zz_slc junk;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "junk"
-- end-expected-error
CLOSE ALL junk;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ","
-- end-expected-error
LISTEN zz_l1, zz_l2;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "'zz_lit'"
-- end-expected-error
LISTEN 'zz_lit';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "'zz_lit'"
-- end-expected-error
NOTIFY 'zz_lit';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "'zz_lit'"
-- end-expected-error
UNLISTEN 'zz_lit';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "select"
-- end-expected-error
LISTEN select;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "all"
-- end-expected-error
NOTIFY all;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "table"
-- end-expected-error
UNLISTEN table;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "||"
-- end-expected-error
NOTIFY zz_ex, 'a' || 'b';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ","
-- end-expected-error
NOTIFY zz_ex, 'a', 'b';

NOTIFY zz_dq, $$hi$$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "BOGUS"
-- end-expected-error
DISCARD BOGUS;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "SEQUENCE"
-- end-expected-error
DISCARD SEQUENCE;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "EXTRA"
-- end-expected-error
DISCARD ALL EXTRA;

LISTEN "Zz_Mixed";

-- begin-expected
-- columns: c
-- row: Zz_Mixed
-- end-expected
SELECT c FROM pg_listening_channels() c;

UNLISTEN "zz_mixed";

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_listening_channels();

UNLISTEN "Zz_Mixed";

SET zz_x.k = 'v';

RESET ALL;

-- begin-expected
-- columns: v|isnull
-- row: |f
-- end-expected
SELECT current_setting('zz_x.k', true) AS v, current_setting('zz_x.k', true) IS NULL AS isnull;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at end of input
-- end-expected-error
DO;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: language "nosuchlang_zz" does not exist
-- end-expected-error
DO LANGUAGE nosuchlang_zz $$ BEGIN NULL; END $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: language "sql" does not support inline code execution
-- end-expected-error
DO LANGUAGE sql $$ SELECT 1 $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: language "c" does not support inline code execution
-- end-expected-error
DO LANGUAGE c $$ BEGIN NULL; END $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: language "internal" does not support inline code execution
-- end-expected-error
DO LANGUAGE internal $$ BEGIN NULL; END $$;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: language "PLPGSQL" does not exist
-- end-expected-error
DO LANGUAGE "PLPGSQL" $$ BEGIN NULL; END $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: language "sql" does not support inline code execution
-- end-expected-error
DO $$ BEGIN NULL; END $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: conflicting or redundant options
-- end-expected-error
DO $$ BEGIN NULL; END $$ LANGUAGE plpgsql LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at end of input
-- end-expected-error
DO $$ $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at end of input
-- end-expected-error
DO '';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "42"
-- end-expected-error
DO 42;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "EXTRA"
-- end-expected-error
DO $$ BEGIN NULL; END $$ EXTRA;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "END"
-- end-expected-error
DO $$ BEGIN NULL END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at end of input
-- end-expected-error
DO $$ DECLARE x int; BEGIN x := 1 END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: query has no destination for result data
-- end-expected-error
DO $$ BEGIN SELECT 1; END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: ERROR: RETURN cannot have a parameter in function returning void
-- end-expected-error
DO $$ BEGIN RETURN 1; END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: ERROR: cannot use RETURN NEXT in a non-SETOF function
-- end-expected-error
DO $$ DECLARE x int; BEGIN x := 1; RETURN NEXT x; END $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: ERROR: cannot use RETURN QUERY in a non-SETOF function
-- end-expected-error
DO $$ BEGIN RETURN QUERY SELECT 1; END $$;

DO $$ BEGIN PERFORM 1; END $$;

DO $$ DECLARE x int; BEGIN SELECT 1 INTO x; END $$;

-- begin-expected-error
-- sqlstate: 25001
-- message-like: ERROR: DISCARD ALL cannot be executed from a function
-- end-expected-error
DO $$ BEGIN EXECUTE 'DISCARD ALL'; END $$;

CREATE TABLE zz_sl_vt (a int);

-- begin-expected-error
-- sqlstate: 25001
-- message-like: ERROR: VACUUM cannot be executed from a function
-- end-expected-error
DO $$ BEGIN VACUUM zz_sl_vt; END $$;

BEGIN;

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1;

SET TRANSACTION READ WRITE;

SET TRANSACTION READ ONLY;

-- begin-expected-error
-- sqlstate: 25001
-- message-like: ERROR: transaction read-write mode must be set before any query
-- end-expected-error
SET TRANSACTION READ WRITE;

ROLLBACK;

DROP TABLE zz_sl_vt;

