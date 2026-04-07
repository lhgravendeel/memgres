DROP SCHEMA IF EXISTS test_190 CASCADE;
CREATE SCHEMA test_190;
SET search_path TO test_190;

CREATE TABLE proc_log (
    id integer PRIMARY KEY,
    msg text NOT NULL
);

CREATE PROCEDURE add_log(p_id integer, p_msg text)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO proc_log(id, msg) VALUES (p_id, p_msg);
END;
$$;

CALL add_log(1, 'first');

DO $$
DECLARE
    i integer;
BEGIN
    FOR i IN 2..4 LOOP
        INSERT INTO proc_log(id, msg) VALUES (i, 'generated-' || i);
    END LOOP;
END;
$$;

-- begin-expected
-- columns: id|msg
-- row: 1|first
-- row: 2|generated-2
-- row: 3|generated-3
-- row: 4|generated-4
-- end-expected
SELECT id, msg
FROM proc_log
ORDER BY id;

DROP SCHEMA test_190 CASCADE;
