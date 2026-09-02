-- source: investigation-2026-08.md
-- finding: 202
-- title: The DO grammar reads two tokens and discards them, and the body is never compiled. parseDo reads an optional LANGUAGE identifier and throws it away with no look
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
DO;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "nosuchlang_zz" does not exist
-- end-expected-error
DO LANGUAGE nosuchlang_zz $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: language "sql" does not support inline code execution
-- end-expected-error
DO LANGUAGE sql $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: language "c" does not support inline code execution
-- end-expected-error
DO LANGUAGE c $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: language "internal" does not support inline code execution
-- end-expected-error
DO LANGUAGE internal $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "PLPGSQL" does not exist
-- end-expected-error
DO LANGUAGE "PLPGSQL" $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: language "sql" does not support inline code execution
-- end-expected-error
DO $$ BEGIN NULL; END $$ LANGUAGE sql;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
DO $$ BEGIN NULL; END $$ LANGUAGE plpgsql LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
DO $$ $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
DO '';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "42"
-- end-expected-error
DO 42;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXTRA"
-- end-expected-error
DO $$ BEGIN NULL; END $$ EXTRA;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "END"
-- end-expected-error
DO $$ BEGIN NULL END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
DO $$ DECLARE x int; BEGIN x := 1 END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: query has no destination for result data
-- end-expected-error
DO $$ BEGIN SELECT 1; END $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: RETURN cannot have a parameter in function returning void
-- end-expected-error
DO $$ BEGIN RETURN 1; END $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot use RETURN NEXT in a non-SETOF function
-- end-expected-error
DO $$ DECLARE x int; BEGIN x := 1; RETURN NEXT x; END $$;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot use RETURN QUERY in a non-SETOF function
-- end-expected-error
DO $$ BEGIN RETURN QUERY SELECT 1; END $$;
