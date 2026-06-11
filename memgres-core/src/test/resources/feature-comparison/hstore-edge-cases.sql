-- ============================================================================
-- Feature Comparison: hstore Edge Cases
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests hstore behavior in advanced contexts:
--   A. Without CREATE EXTENSION (should fail)
--   B. CASE expressions with hstore operators
--   C. PL/pgSQL functions using hstore
--   D. SQL-language functions using hstore
--   E. Expression indexes on hstore columns
--   F. Default values using hstore functions
--   G. CHECK constraints with hstore
--   H. GENERATED columns with hstore expressions
--   I. Triggers on hstore columns
--   J. CTEs and subqueries with hstore
--   K. Views over hstore columns
--   L. PREPARE / EXECUTE with hstore
--   M. DO blocks with hstore
--   N. hstore in aggregate expressions
--   O. hstore with UNION/INTERSECT/EXCEPT
-- ============================================================================

-- ============================================================================
-- A. Without CREATE EXTENSION — hstore should not be available
-- ============================================================================

DROP SCHEMA IF EXISTS hs_noext CASCADE;
CREATE SCHEMA hs_noext;
SET search_path = hs_noext;

-- A1. CREATE TABLE with hstore type should fail without extension
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "hstore" does not exist
-- end-expected-error
CREATE TABLE hs_noext.t_noext (id int PRIMARY KEY, data hstore);

-- A2. hstore literal cast should fail without extension
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "hstore" does not exist
-- end-expected-error
SELECT 'a=>1'::hstore;

-- A3. hstore function should fail without extension
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function hstore
-- end-expected-error
SELECT hstore('a', '1');

-- A4. exist() function should fail without extension
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function exist
-- end-expected-error
SELECT exist('a=>1', 'a');

-- A5. akeys() function should fail without extension
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function akeys
-- end-expected-error
SELECT akeys('a=>1');

-- Now install the extension for the remaining tests
SET search_path = hs_noext, public;
CREATE EXTENSION IF NOT EXISTS hstore;

-- ============================================================================
-- B. CASE expressions with hstore operators
-- ============================================================================

CREATE TABLE hs_noext.t_case (id int PRIMARY KEY, data hstore);
INSERT INTO hs_noext.t_case VALUES
    (1, 'status=>active, priority=>high'),
    (2, 'status=>inactive, priority=>low'),
    (3, 'status=>active, priority=>low');

-- B1. CASE with arrow operator
-- begin-expected
-- columns: id|label
-- row: 1|ACTIVE
-- row: 2|INACTIVE
-- row: 3|ACTIVE
-- end-expected
SELECT id, CASE WHEN data->'status' = 'active' THEN 'ACTIVE' ELSE 'INACTIVE' END AS label
FROM hs_noext.t_case ORDER BY id;

-- B2. CASE with contains operator
-- begin-expected
-- columns: id|is_high
-- row: 1|yes
-- row: 2|no
-- row: 3|no
-- end-expected
SELECT id, CASE WHEN data @> 'priority=>high' THEN 'yes' ELSE 'no' END AS is_high
FROM hs_noext.t_case ORDER BY id;

-- B3. CASE returning hstore values
-- begin-expected
-- columns: id|result
-- row: 1|high
-- row: 2|unknown
-- row: 3|low
-- end-expected
SELECT id,
    (CASE WHEN data->'status' = 'active'
          THEN data
          ELSE 'priority=>unknown'::hstore
     END)->'priority' AS result
FROM hs_noext.t_case ORDER BY id;

-- B4. Nested CASE with exist()
-- begin-expected
-- columns: id|has_status
-- row: 1|yes
-- row: 2|yes
-- row: 3|yes
-- end-expected
SELECT id, CASE WHEN exist(data, 'status') THEN 'yes' ELSE 'no' END AS has_status
FROM hs_noext.t_case ORDER BY id;

-- ============================================================================
-- C. PL/pgSQL functions using hstore
-- ============================================================================

-- C1. Function that extracts a key with fallback
CREATE OR REPLACE FUNCTION hs_noext.hstore_get_or_default(h hstore, k text, def text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF exist(h, k) AND defined(h, k) THEN
        RETURN h->k;
    ELSE
        RETURN def;
    END IF;
END;
$$;

-- begin-expected
-- columns: val
-- row: hello
-- end-expected
SELECT hs_noext.hstore_get_or_default('a=>hello'::hstore, 'a', 'fallback') AS val;

-- begin-expected
-- columns: val
-- row: fallback
-- end-expected
SELECT hs_noext.hstore_get_or_default('a=>hello'::hstore, 'missing', 'fallback') AS val;

-- C2a. Using reserved keyword "overlay" as parameter name should fail (PG rejects this)
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error
-- end-expected-error
CREATE OR REPLACE FUNCTION hs_noext.hstore_merge_with_prefix(base hstore, overlay hstore, prefix text)
RETURNS hstore LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    result hstore;
    k text;
    v text;
BEGIN
    result := base;
    FOR k, v IN SELECT * FROM each(overlay)
    LOOP
        IF exist(base, k) THEN
            result := result || hstore(prefix || k, v);
        ELSE
            result := result || hstore(k, v);
        END IF;
    END LOOP;
    RETURN result;
END;
$$;

-- C2b. Multi-variable FOR loop with each() — FOR k, v IN SELECT * FROM each(h)
CREATE OR REPLACE FUNCTION hs_noext.hstore_merge_with_prefix(base hstore, additions hstore, prefix text)
RETURNS hstore LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    result hstore;
    k text;
    v text;
BEGIN
    result := base;
    FOR k, v IN SELECT * FROM each(additions)
    LOOP
        IF exist(base, k) THEN
            result := result || hstore(prefix || k, v);
        ELSE
            result := result || hstore(k, v);
        END IF;
    END LOOP;
    RETURN result;
END;
$$;

-- begin-expected
-- columns: val
-- row: original
-- end-expected
SELECT (hs_noext.hstore_merge_with_prefix('a=>original'::hstore, 'a=>new'::hstore, 'override_'))->'a' AS val;

-- C3. Function returning hstore from computation
CREATE OR REPLACE FUNCTION hs_noext.build_config(env text, debug boolean)
RETURNS hstore LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN hstore('env', env) || hstore('debug', debug::text);
END;
$$;

-- begin-expected
-- columns: env|debug
-- row: prod|false
-- end-expected
SELECT (hs_noext.build_config('prod', false))->'env' AS env,
       (hs_noext.build_config('prod', false))->'debug' AS debug;

-- C4. Function with hstore loop using skeys
CREATE OR REPLACE FUNCTION hs_noext.count_non_null_values(h hstore)
RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    cnt int := 0;
    k text;
BEGIN
    FOR k IN SELECT skeys(h)
    LOOP
        IF defined(h, k) THEN
            cnt := cnt + 1;
        END IF;
    END LOOP;
    RETURN cnt;
END;
$$;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT hs_noext.count_non_null_values('a=>1, b=>NULL, c=>3'::hstore) AS cnt;

-- ============================================================================
-- D. SQL-language functions using hstore
-- ============================================================================

-- D1. Simple SQL function extracting key
CREATE OR REPLACE FUNCTION hs_noext.get_status(h hstore)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT h->'status';
$$;

-- begin-expected
-- columns: val
-- row: active
-- end-expected
SELECT hs_noext.get_status('status=>active, name=>test'::hstore) AS val;

-- D2. SQL function returning boolean via contains
CREATE OR REPLACE FUNCTION hs_noext.is_admin(h hstore)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT h @> 'role=>admin'::hstore;
$$;

-- begin-expected
-- columns: val
-- row: true
-- end-expected
SELECT hs_noext.is_admin('role=>admin, name=>bob'::hstore) AS val;

-- begin-expected
-- columns: val
-- row: false
-- end-expected
SELECT hs_noext.is_admin('role=>user, name=>alice'::hstore) AS val;

-- D3. SQL function with hstore concat
CREATE OR REPLACE FUNCTION hs_noext.add_timestamp(h hstore)
RETURNS hstore LANGUAGE sql IMMUTABLE AS $$
    SELECT h || hstore('updated', 'now');
$$;

-- begin-expected
-- columns: val
-- row: now
-- end-expected
SELECT (hs_noext.add_timestamp('a=>1'::hstore))->'updated' AS val;

-- ============================================================================
-- E. Expression indexes on hstore columns
-- ============================================================================

CREATE TABLE hs_noext.t_idx (id int PRIMARY KEY, data hstore);
INSERT INTO hs_noext.t_idx VALUES
    (1, 'email=>alice@test.com, name=>alice'),
    (2, 'email=>bob@test.com, name=>bob'),
    (3, 'email=>charlie@test.com, name=>charlie');

-- E1. Create expression index on hstore arrow extraction
CREATE INDEX idx_hs_email ON hs_noext.t_idx ((data->'email'));

-- E2. Query using the indexed expression
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM hs_noext.t_idx WHERE data->'email' = 'bob@test.com';

-- E3. Create expression index on exist() function
CREATE INDEX idx_hs_has_phone ON hs_noext.t_idx ((exist(data, 'phone')));

-- E4. Verify index exists in pg_indexes
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM pg_indexes
WHERE schemaname = 'hs_noext' AND tablename = 't_idx' AND indexname LIKE 'idx_hs_%';

-- ============================================================================
-- F. Default values using hstore functions/constructors
-- ============================================================================

-- F1. Column default with hstore literal
CREATE TABLE hs_noext.t_default1 (id int PRIMARY KEY, config hstore DEFAULT 'log_level=>info, debug=>false');
INSERT INTO hs_noext.t_default1 (id) VALUES (1);

-- begin-expected
-- columns: val
-- row: info
-- end-expected
SELECT config->'log_level' AS val FROM hs_noext.t_default1 WHERE id = 1;

-- F2. Column default with hstore constructor function
CREATE TABLE hs_noext.t_default2 (id int PRIMARY KEY, meta hstore DEFAULT hstore('created_by', 'system'));
INSERT INTO hs_noext.t_default2 (id) VALUES (1);

-- begin-expected
-- columns: val
-- row: system
-- end-expected
SELECT meta->'created_by' AS val FROM hs_noext.t_default2 WHERE id = 1;

-- F3. Column default with empty hstore
CREATE TABLE hs_noext.t_default3 (id int PRIMARY KEY, tags hstore DEFAULT ''::hstore);
INSERT INTO hs_noext.t_default3 (id) VALUES (1);

-- begin-expected
-- columns: is_empty
-- row: true
-- end-expected
SELECT (tags->'anything') IS NULL AS is_empty FROM hs_noext.t_default3 WHERE id = 1;

-- ============================================================================
-- G. CHECK constraints with hstore
-- ============================================================================

-- G1. CHECK constraint using exist() — key must be present
CREATE TABLE hs_noext.t_check1 (
    id int PRIMARY KEY,
    data hstore,
    CONSTRAINT must_have_name CHECK (exist(data, 'name'))
);

INSERT INTO hs_noext.t_check1 VALUES (1, 'name=>alice, age=>30');

-- begin-expected
-- columns: val
-- row: alice
-- end-expected
SELECT data->'name' AS val FROM hs_noext.t_check1 WHERE id = 1;

-- G2. CHECK constraint violation
-- begin-expected-error
-- sqlstate: 23514
-- message-like: must_have_name
-- end-expected-error
INSERT INTO hs_noext.t_check1 VALUES (2, 'age=>25');

-- G3. CHECK constraint using arrow comparison
CREATE TABLE hs_noext.t_check2 (
    id int PRIMARY KEY,
    settings hstore,
    CONSTRAINT valid_log_level CHECK (settings->'log_level' IN ('debug', 'info', 'warn', 'error'))
);

INSERT INTO hs_noext.t_check2 VALUES (1, 'log_level=>info');

-- begin-expected-error
-- sqlstate: 23514
-- message-like: valid_log_level
-- end-expected-error
INSERT INTO hs_noext.t_check2 VALUES (2, 'log_level=>trace');

-- ============================================================================
-- H. GENERATED columns with hstore expressions
-- ============================================================================

-- H1. Generated column extracting a key from hstore
CREATE TABLE hs_noext.t_gen (
    id int PRIMARY KEY,
    data hstore,
    status text GENERATED ALWAYS AS (data->'status') STORED
);

INSERT INTO hs_noext.t_gen VALUES (1, 'status=>active, name=>test');

-- begin-expected
-- columns: status
-- row: active
-- end-expected
SELECT status FROM hs_noext.t_gen WHERE id = 1;

-- H2. Update hstore updates generated column
UPDATE hs_noext.t_gen SET data = 'status=>inactive, name=>test' WHERE id = 1;

-- begin-expected
-- columns: status
-- row: inactive
-- end-expected
SELECT status FROM hs_noext.t_gen WHERE id = 1;

-- ============================================================================
-- I. Triggers on hstore columns
-- ============================================================================

CREATE TABLE hs_noext.t_trig (id int PRIMARY KEY, data hstore);
CREATE TABLE hs_noext.t_trig_log (id serial PRIMARY KEY, action text, old_data text, new_data text);

-- I1. Trigger function that logs hstore changes
CREATE OR REPLACE FUNCTION hs_noext.log_hstore_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO hs_noext.t_trig_log (action, old_data, new_data)
        VALUES ('INSERT', NULL, NEW.data::text);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO hs_noext.t_trig_log (action, old_data, new_data)
        VALUES ('UPDATE', OLD.data::text, NEW.data::text);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO hs_noext.t_trig_log (action, old_data, new_data)
        VALUES ('DELETE', OLD.data::text, NULL);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_hstore_log
    AFTER INSERT OR UPDATE OR DELETE ON hs_noext.t_trig
    FOR EACH ROW EXECUTE FUNCTION hs_noext.log_hstore_change();

INSERT INTO hs_noext.t_trig VALUES (1, 'a=>1');
UPDATE hs_noext.t_trig SET data = 'a=>2' WHERE id = 1;
DELETE FROM hs_noext.t_trig WHERE id = 1;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*) AS cnt FROM hs_noext.t_trig_log;

-- I2. Trigger that modifies hstore on insert
CREATE OR REPLACE FUNCTION hs_noext.add_created_key()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.data := NEW.data || hstore('auto_added', 'true');
    RETURN NEW;
END;
$$;

CREATE TABLE hs_noext.t_trig2 (id int PRIMARY KEY, data hstore);
CREATE TRIGGER trg_add_key
    BEFORE INSERT ON hs_noext.t_trig2
    FOR EACH ROW EXECUTE FUNCTION hs_noext.add_created_key();

INSERT INTO hs_noext.t_trig2 VALUES (1, 'name=>test');

-- begin-expected
-- columns: val
-- row: true
-- end-expected
SELECT data->'auto_added' AS val FROM hs_noext.t_trig2 WHERE id = 1;

-- ============================================================================
-- J. CTEs and subqueries with hstore
-- ============================================================================

CREATE TABLE hs_noext.t_cte (id int PRIMARY KEY, data hstore);
INSERT INTO hs_noext.t_cte VALUES
    (1, 'type=>fruit, name=>apple'),
    (2, 'type=>vegetable, name=>carrot'),
    (3, 'type=>fruit, name=>banana');

-- J1. CTE filtering by hstore arrow
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
WITH fruits AS (
    SELECT * FROM hs_noext.t_cte WHERE data->'type' = 'fruit'
)
SELECT count(*) AS cnt FROM fruits;

-- J2. Correlated subquery with hstore
-- begin-expected
-- columns: id|same_type_count
-- row: 1|2
-- row: 2|1
-- row: 3|2
-- end-expected
SELECT t.id,
    (SELECT count(*) FROM hs_noext.t_cte t2 WHERE t2.data->'type' = t.data->'type') AS same_type_count
FROM hs_noext.t_cte t ORDER BY t.id;

-- J3. CTE with hstore aggregation
-- begin-expected
-- columns: type|cnt
-- row: fruit|2
-- row: vegetable|1
-- end-expected
WITH typed AS (
    SELECT data->'type' AS type FROM hs_noext.t_cte
)
SELECT type, count(*) AS cnt FROM typed GROUP BY type ORDER BY type;

-- ============================================================================
-- K. Views over hstore columns
-- ============================================================================

CREATE TABLE hs_noext.t_view (id int PRIMARY KEY, data hstore);
INSERT INTO hs_noext.t_view VALUES
    (1, 'name=>alice, role=>admin'),
    (2, 'name=>bob, role=>user');

-- K1. Create view that extracts hstore keys
CREATE VIEW hs_noext.v_users AS
    SELECT id, data->'name' AS name, data->'role' AS role FROM hs_noext.t_view;

-- begin-expected
-- columns: id|name|role
-- row: 1|alice|admin
-- row: 2|bob|user
-- end-expected
SELECT * FROM hs_noext.v_users ORDER BY id;

-- K2. Query view with filter
-- begin-expected
-- columns: name
-- row: alice
-- end-expected
SELECT name FROM hs_noext.v_users WHERE role = 'admin';

-- K3. View with hstore contains check
CREATE VIEW hs_noext.v_admins AS
    SELECT id, data->'name' AS name FROM hs_noext.t_view WHERE data @> 'role=>admin';

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM hs_noext.v_admins;

-- ============================================================================
-- L. PREPARE / EXECUTE with hstore
-- ============================================================================

CREATE TABLE hs_noext.t_prep (id int PRIMARY KEY, data hstore);
INSERT INTO hs_noext.t_prep VALUES (1, 'x=>10, y=>20'), (2, 'x=>30, y=>40');

-- L1. Prepared statement with hstore column
PREPARE hs_fetch(int) AS SELECT data->'x' AS val FROM hs_noext.t_prep WHERE id = $1;

-- begin-expected
-- columns: val
-- row: 10
-- end-expected
EXECUTE hs_fetch(1);

-- begin-expected
-- columns: val
-- row: 30
-- end-expected
EXECUTE hs_fetch(2);

DEALLOCATE hs_fetch;

-- L2. Prepared insert with hstore
PREPARE hs_ins(int, text) AS INSERT INTO hs_noext.t_prep VALUES ($1, $2::hstore);
EXECUTE hs_ins(3, 'x=>50, y=>60');

-- begin-expected
-- columns: val
-- row: 50
-- end-expected
SELECT data->'x' AS val FROM hs_noext.t_prep WHERE id = 3;

DEALLOCATE hs_ins;

-- ============================================================================
-- M. DO blocks with hstore
-- ============================================================================

CREATE TABLE hs_noext.t_do (id int PRIMARY KEY, data hstore);

-- M1. DO block inserting hstore data
DO $$
BEGIN
    INSERT INTO hs_noext.t_do VALUES (1, 'from_do=>true');
END;
$$;

-- begin-expected
-- columns: val
-- row: true
-- end-expected
SELECT data->'from_do' AS val FROM hs_noext.t_do WHERE id = 1;

-- M2. DO block with hstore variable
DO $$
DECLARE
    h hstore;
BEGIN
    h := 'key1=>val1, key2=>val2'::hstore;
    INSERT INTO hs_noext.t_do VALUES (2, h);
END;
$$;

-- begin-expected
-- columns: val
-- row: val2
-- end-expected
SELECT data->'key2' AS val FROM hs_noext.t_do WHERE id = 2;

-- M3. DO block with hstore concatenation
DO $$
DECLARE
    base hstore := 'a=>1'::hstore;
    extra hstore := 'b=>2'::hstore;
BEGIN
    INSERT INTO hs_noext.t_do VALUES (3, base || extra);
END;
$$;

-- begin-expected
-- columns: a|b
-- row: 1|2
-- end-expected
SELECT data->'a' AS a, data->'b' AS b FROM hs_noext.t_do WHERE id = 3;

-- ============================================================================
-- N. hstore in aggregate expressions
-- ============================================================================

CREATE TABLE hs_noext.t_agg (id int PRIMARY KEY, data hstore);
INSERT INTO hs_noext.t_agg VALUES
    (1, 'score=>85, team=>a'),
    (2, 'score=>92, team=>b'),
    (3, 'score=>78, team=>a'),
    (4, 'score=>95, team=>b');

-- N1. Aggregate on extracted hstore values
-- begin-expected
-- columns: team|avg_score
-- row: a|81.5000000000000000
-- row: b|93.5000000000000000
-- end-expected
SELECT data->'team' AS team,
       avg((data->'score')::numeric) AS avg_score
FROM hs_noext.t_agg GROUP BY data->'team' ORDER BY data->'team';

-- N2. Count with hstore condition
-- begin-expected
-- columns: high_scorers
-- row: 2
-- end-expected
SELECT count(*) AS high_scorers FROM hs_noext.t_agg WHERE (data->'score')::int > 90;

-- N3. String aggregation of hstore keys
-- begin-expected
-- columns: all_teams
-- row: a,b
-- end-expected
SELECT string_agg(DISTINCT data->'team', ',' ORDER BY data->'team') AS all_teams FROM hs_noext.t_agg;

-- ============================================================================
-- O. hstore with UNION / INTERSECT / EXCEPT
-- ============================================================================

CREATE TABLE hs_noext.t_set1 (id int PRIMARY KEY, data hstore);
CREATE TABLE hs_noext.t_set2 (id int PRIMARY KEY, data hstore);
INSERT INTO hs_noext.t_set1 VALUES (1, 'a=>1'), (2, 'b=>2');
INSERT INTO hs_noext.t_set2 VALUES (3, 'b=>2'), (4, 'c=>3');

-- O1. UNION on hstore columns
-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*) AS cnt FROM (
    SELECT data FROM hs_noext.t_set1
    UNION
    SELECT data FROM hs_noext.t_set2
) sub;

-- O2. INTERSECT on hstore columns (b=>2 appears in both)
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM (
    SELECT data FROM hs_noext.t_set1
    INTERSECT
    SELECT data FROM hs_noext.t_set2
) sub;

-- O3. EXCEPT on hstore columns
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM (
    SELECT data FROM hs_noext.t_set1
    EXCEPT
    SELECT data FROM hs_noext.t_set2
) sub;

-- ============================================================================
-- Cleanup
-- ============================================================================

SET search_path = public;
DROP SCHEMA IF EXISTS hs_noext CASCADE;
