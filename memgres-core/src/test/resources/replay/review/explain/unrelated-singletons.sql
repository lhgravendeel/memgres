-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: EXPLAIN
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_p() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "RETURNING"
-- end-expected-error
CALL zz_p() RETURNING 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
CALL zz_p() 1;
-- begin-expected-error
-- sqlstate: 25001
-- message-like: SET TRANSACTION ISOLATION LEVEL must be called before any query
-- end-expected-error
DO $$ BEGIN SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; END $$;
