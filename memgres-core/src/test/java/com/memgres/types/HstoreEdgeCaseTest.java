package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests hstore behavior in advanced contexts:
 *  - Without CREATE EXTENSION (should fail)
 *  - CASE expressions, PL/pgSQL, SQL functions
 *  - Expression indexes, defaults, CHECK constraints
 *  - GENERATED columns, triggers, CTEs, views
 *  - PREPARE/EXECUTE, DO blocks, aggregates, set operations
 */
class HstoreEdgeCaseTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertFalse(rs.next(), "expected single row");
            return v;
        }
    }

    private static boolean bool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private static int count(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. Without CREATE EXTENSION
    // =========================================================================

    @Test
    void hstore_type_fails_without_extension() throws SQLException {
        exec("DROP EXTENSION IF EXISTS hstore CASCADE");
        try {
            // hstore type should not be available without CREATE EXTENSION
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("CREATE TABLE no_ext_test (id int, data hstore)"));
            assertTrue(ex.getMessage().contains("does not exist") || ex.getMessage().contains("42704"),
                    "Should fail with type not found: " + ex.getMessage());
        } finally {
            exec("CREATE EXTENSION IF NOT EXISTS hstore");
        }
    }

    @Test
    void hstore_cast_fails_without_extension() throws SQLException {
        exec("DROP EXTENSION IF EXISTS hstore CASCADE");
        try {
            SQLException ex = assertThrows(SQLException.class, () ->
                    str("SELECT 'a=>1'::hstore"));
            assertTrue(ex.getMessage().contains("does not exist") || ex.getMessage().contains("42704"),
                    "Should fail with type not found: " + ex.getMessage());
        } finally {
            exec("CREATE EXTENSION IF NOT EXISTS hstore");
        }
    }

    @Test
    void hstore_function_fails_without_extension() throws SQLException {
        exec("DROP EXTENSION IF EXISTS hstore CASCADE");
        try {
            SQLException ex = assertThrows(SQLException.class, () ->
                    str("SELECT hstore('a', '1')"));
            assertTrue(ex.getMessage().contains("does not exist") || ex.getMessage().contains("42883"),
                    "Should fail with function not found: " + ex.getMessage());
        } finally {
            exec("CREATE EXTENSION IF NOT EXISTS hstore");
        }
    }

    @Test
    void exist_function_fails_without_extension() throws SQLException {
        exec("DROP EXTENSION IF EXISTS hstore CASCADE");
        try {
            SQLException ex = assertThrows(SQLException.class, () ->
                    str("SELECT exist('a=>1', 'a')"));
            assertTrue(ex.getMessage().contains("does not exist") || ex.getMessage().contains("42883"),
                    "Should fail with function not found: " + ex.getMessage());
        } finally {
            exec("CREATE EXTENSION IF NOT EXISTS hstore");
        }
    }

    // =========================================================================
    // B. CASE expressions with hstore (extension installed for remaining tests)
    // =========================================================================

    @BeforeEach
    void ensureExtension() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
    }

    @Test
    void case_with_arrow_operator() throws SQLException {
        exec("CREATE TABLE hs_ec_case (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_case VALUES (1, 'status=>active'), (2, 'status=>inactive')");
            String val = str("SELECT CASE WHEN data->'status' = 'active' THEN 'YES' ELSE 'NO' END FROM hs_ec_case WHERE id = 1");
            assertEquals("YES", val);
            val = str("SELECT CASE WHEN data->'status' = 'active' THEN 'YES' ELSE 'NO' END FROM hs_ec_case WHERE id = 2");
            assertEquals("NO", val);
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_case");
        }
    }

    @Test
    void case_with_contains_operator() throws SQLException {
        exec("CREATE TABLE hs_ec_case2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_case2 VALUES (1, 'role=>admin'), (2, 'role=>user')");
            assertTrue(bool("SELECT CASE WHEN data @> 'role=>admin' THEN true ELSE false END FROM hs_ec_case2 WHERE id = 1"));
            assertFalse(bool("SELECT CASE WHEN data @> 'role=>admin' THEN true ELSE false END FROM hs_ec_case2 WHERE id = 2"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_case2");
        }
    }

    @Test
    void case_returning_hstore() throws SQLException {
        String val = str("SELECT (CASE WHEN true THEN 'a=>1'::hstore ELSE 'b=>2'::hstore END)->'a'");
        assertEquals("1", val);
    }

    @Test
    void case_with_exist_function() throws SQLException {
        assertTrue(bool("SELECT CASE WHEN exist('a=>1'::hstore, 'a') THEN true ELSE false END"));
        assertFalse(bool("SELECT CASE WHEN exist('a=>1'::hstore, 'z') THEN true ELSE false END"));
    }

    // =========================================================================
    // C. PL/pgSQL functions using hstore
    // =========================================================================

    @Test
    void plpgsql_function_with_hstore_param() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_hs_get(h hstore, k text, def text) "
                + "RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$ "
                + "BEGIN IF exist(h, k) AND defined(h, k) THEN RETURN h->k; ELSE RETURN def; END IF; END; $$");
        try {
            assertEquals("hello", str("SELECT test_hs_get('a=>hello'::hstore, 'a', 'fallback')"));
            assertEquals("fallback", str("SELECT test_hs_get('a=>hello'::hstore, 'missing', 'fallback')"));
            assertEquals("fallback", str("SELECT test_hs_get('a=>NULL'::hstore, 'a', 'fallback')"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_hs_get");
        }
    }

    @Test
    void plpgsql_function_returning_hstore() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_build_hs(k text, v text) "
                + "RETURNS hstore LANGUAGE plpgsql IMMUTABLE AS $$ "
                + "BEGIN RETURN hstore(k, v); END; $$");
        try {
            assertEquals("hello", str("SELECT (test_build_hs('greeting', 'hello'))->'greeting'"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_build_hs");
        }
    }

    @Test
    void plpgsql_function_with_hstore_loop() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_count_defined(h hstore) "
                + "RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$ "
                + "DECLARE cnt int := 0; k text; "
                + "BEGIN FOR k IN SELECT skeys(h) LOOP "
                + "IF defined(h, k) THEN cnt := cnt + 1; END IF; "
                + "END LOOP; RETURN cnt; END; $$");
        try {
            assertEquals("2", str("SELECT test_count_defined('a=>1, b=>NULL, c=>3'::hstore)"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_count_defined");
        }
    }

    @Test
    void plpgsql_function_hstore_concat() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_hs_merge(a hstore, b hstore) "
                + "RETURNS hstore LANGUAGE plpgsql IMMUTABLE AS $$ "
                + "BEGIN RETURN a || b; END; $$");
        try {
            assertEquals("new", str("SELECT (test_hs_merge('a=>old'::hstore, 'a=>new'::hstore))->'a'"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_hs_merge");
        }
    }

    @Test
    void plpgsql_function_rejects_overlay_as_param_name() {
        // "overlay" is a reserved keyword in PG — cannot be used as a bare parameter name
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE OR REPLACE FUNCTION test_overlay_param(base hstore, overlay hstore) "
                        + "RETURNS hstore LANGUAGE plpgsql IMMUTABLE AS $$ "
                        + "BEGIN RETURN base || overlay; END; $$"));
        assertTrue(ex.getMessage().contains("syntax error") || ex.getSQLState().equals("42601"),
                "Should fail with syntax error for reserved keyword 'overlay': " + ex.getMessage());
    }

    @Test
    void plpgsql_function_merge_with_prefix() throws SQLException {
        // Same function as overlay test but with renamed parameter
        exec("CREATE OR REPLACE FUNCTION test_hs_merge_prefix(base hstore, additions hstore, prefix text) "
                + "RETURNS hstore LANGUAGE plpgsql IMMUTABLE AS $$ "
                + "DECLARE result hstore; k text; "
                + "BEGIN result := base; "
                + "FOR k IN SELECT skeys(additions) LOOP "
                + "IF exist(base, k) THEN result := result || hstore(prefix || k, additions->k); "
                + "ELSE result := result || hstore(k, additions->k); END IF; "
                + "END LOOP; RETURN result; END; $$");
        try {
            assertEquals("original", str("SELECT (test_hs_merge_prefix('a=>original'::hstore, 'a=>new'::hstore, 'override_'))->'a'"));
            assertEquals("new", str("SELECT (test_hs_merge_prefix('a=>original'::hstore, 'a=>new'::hstore, 'override_'))->'override_a'"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_hs_merge_prefix");
        }
    }

    // =========================================================================
    // D. SQL-language functions using hstore
    // =========================================================================

    @Test
    void sql_function_extracting_key() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_sql_get_status(h hstore) "
                + "RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT h->'status'; $$");
        try {
            assertEquals("active", str("SELECT test_sql_get_status('status=>active'::hstore)"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_sql_get_status");
        }
    }

    @Test
    void sql_function_with_contains() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_sql_is_admin(h hstore) "
                + "RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT h @> 'role=>admin'::hstore; $$");
        try {
            assertTrue(bool("SELECT test_sql_is_admin('role=>admin'::hstore)"));
            assertFalse(bool("SELECT test_sql_is_admin('role=>user'::hstore)"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_sql_is_admin");
        }
    }

    @Test
    void sql_function_returning_hstore() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_sql_add_key(h hstore) "
                + "RETURNS hstore LANGUAGE sql IMMUTABLE AS $$ SELECT h || hstore('added', 'true'); $$");
        try {
            assertEquals("true", str("SELECT (test_sql_add_key('a=>1'::hstore))->'added'"));
        } finally {
            exec("DROP FUNCTION IF EXISTS test_sql_add_key");
        }
    }

    // =========================================================================
    // E. Expression indexes on hstore columns
    // =========================================================================

    @Test
    void expression_index_on_hstore_arrow() throws SQLException {
        exec("CREATE TABLE hs_ec_idx (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_idx VALUES (1, 'email=>a@test.com'), (2, 'email=>b@test.com')");
            exec("CREATE INDEX idx_hs_ec_email ON hs_ec_idx ((data->'email'))");
            assertEquals("2", str("SELECT id FROM hs_ec_idx WHERE data->'email' = 'b@test.com'"));
            assertEquals("1", str("SELECT count(*) FROM pg_indexes WHERE tablename = 'hs_ec_idx' AND indexname = 'idx_hs_ec_email'"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_idx");
        }
    }

    @Test
    void expression_index_on_exist_function() throws SQLException {
        exec("CREATE TABLE hs_ec_idx2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_idx2 VALUES (1, 'phone=>123'), (2, 'email=>a@test.com')");
            exec("CREATE INDEX idx_hs_ec_phone ON hs_ec_idx2 ((exist(data, 'phone')))");
            assertEquals("1", str("SELECT count(*) FROM pg_indexes WHERE tablename = 'hs_ec_idx2' AND indexname = 'idx_hs_ec_phone'"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_idx2");
        }
    }

    // =========================================================================
    // F. Default values using hstore
    // =========================================================================

    @Test
    void default_hstore_literal() throws SQLException {
        exec("CREATE TABLE hs_ec_def1 (id int PRIMARY KEY, config hstore DEFAULT 'log=>info')");
        try {
            exec("INSERT INTO hs_ec_def1 (id) VALUES (1)");
            assertEquals("info", str("SELECT config->'log' FROM hs_ec_def1 WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_def1");
        }
    }

    @Test
    void default_hstore_constructor() throws SQLException {
        exec("CREATE TABLE hs_ec_def2 (id int PRIMARY KEY, meta hstore DEFAULT hstore('by', 'system'))");
        try {
            exec("INSERT INTO hs_ec_def2 (id) VALUES (1)");
            assertEquals("system", str("SELECT meta->'by' FROM hs_ec_def2 WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_def2");
        }
    }

    @Test
    void default_empty_hstore() throws SQLException {
        exec("CREATE TABLE hs_ec_def3 (id int PRIMARY KEY, tags hstore DEFAULT ''::hstore)");
        try {
            exec("INSERT INTO hs_ec_def3 (id) VALUES (1)");
            assertTrue(bool("SELECT (tags->'x') IS NULL FROM hs_ec_def3 WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_def3");
        }
    }

    // =========================================================================
    // G. CHECK constraints with hstore
    // =========================================================================

    @Test
    void check_constraint_with_exist() throws SQLException {
        exec("CREATE TABLE hs_ec_chk1 (id int PRIMARY KEY, data hstore, "
                + "CONSTRAINT must_have_name CHECK (exist(data, 'name')))");
        try {
            exec("INSERT INTO hs_ec_chk1 VALUES (1, 'name=>alice')");
            assertEquals("alice", str("SELECT data->'name' FROM hs_ec_chk1 WHERE id = 1"));
            // Should fail without 'name' key
            assertThrows(SQLException.class, () -> exec("INSERT INTO hs_ec_chk1 VALUES (2, 'age=>25')"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_chk1");
        }
    }

    @Test
    void check_constraint_with_arrow_comparison() throws SQLException {
        exec("CREATE TABLE hs_ec_chk2 (id int PRIMARY KEY, settings hstore, "
                + "CONSTRAINT valid_level CHECK (settings->'level' IN ('low', 'medium', 'high')))");
        try {
            exec("INSERT INTO hs_ec_chk2 VALUES (1, 'level=>high')");
            assertThrows(SQLException.class, () -> exec("INSERT INTO hs_ec_chk2 VALUES (2, 'level=>extreme')"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_chk2");
        }
    }

    // =========================================================================
    // H. GENERATED columns with hstore
    // =========================================================================

    @Test
    void generated_column_from_hstore_arrow() throws SQLException {
        exec("CREATE TABLE hs_ec_gen (id int PRIMARY KEY, data hstore, "
                + "status text GENERATED ALWAYS AS (data->'status') STORED)");
        try {
            exec("INSERT INTO hs_ec_gen VALUES (1, 'status=>active')");
            assertEquals("active", str("SELECT status FROM hs_ec_gen WHERE id = 1"));
            exec("UPDATE hs_ec_gen SET data = 'status=>inactive' WHERE id = 1");
            assertEquals("inactive", str("SELECT status FROM hs_ec_gen WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_gen");
        }
    }

    // =========================================================================
    // I. Triggers on hstore columns
    // =========================================================================

    @Test
    void trigger_logs_hstore_changes() throws SQLException {
        exec("CREATE TABLE hs_ec_trig (id int PRIMARY KEY, data hstore)");
        exec("CREATE TABLE hs_ec_trig_log (id serial, action text, data_text text)");
        exec("CREATE OR REPLACE FUNCTION hs_ec_log_fn() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN IF TG_OP = 'INSERT' THEN INSERT INTO hs_ec_trig_log (action, data_text) VALUES ('I', NEW.data::text); RETURN NEW; "
                + "ELSIF TG_OP = 'UPDATE' THEN INSERT INTO hs_ec_trig_log (action, data_text) VALUES ('U', NEW.data::text); RETURN NEW; "
                + "ELSIF TG_OP = 'DELETE' THEN INSERT INTO hs_ec_trig_log (action, data_text) VALUES ('D', OLD.data::text); RETURN OLD; END IF; RETURN NULL; END; $$");
        exec("CREATE TRIGGER trg_ec_log AFTER INSERT OR UPDATE OR DELETE ON hs_ec_trig "
                + "FOR EACH ROW EXECUTE FUNCTION hs_ec_log_fn()");
        try {
            exec("INSERT INTO hs_ec_trig VALUES (1, 'a=>1')");
            exec("UPDATE hs_ec_trig SET data = 'a=>2' WHERE id = 1");
            exec("DELETE FROM hs_ec_trig WHERE id = 1");
            assertEquals(3, count("SELECT count(*) FROM hs_ec_trig_log"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_trig CASCADE");
            exec("DROP TABLE IF EXISTS hs_ec_trig_log CASCADE");
            exec("DROP FUNCTION IF EXISTS hs_ec_log_fn");
        }
    }

    @Test
    void trigger_modifies_hstore_on_insert() throws SQLException {
        exec("CREATE TABLE hs_ec_trig2 (id int PRIMARY KEY, data hstore)");
        exec("CREATE OR REPLACE FUNCTION hs_ec_add_key_fn() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN NEW.data := NEW.data || hstore('auto', 'true'); RETURN NEW; END; $$");
        exec("CREATE TRIGGER trg_ec_add BEFORE INSERT ON hs_ec_trig2 "
                + "FOR EACH ROW EXECUTE FUNCTION hs_ec_add_key_fn()");
        try {
            exec("INSERT INTO hs_ec_trig2 VALUES (1, 'name=>test')");
            assertEquals("true", str("SELECT data->'auto' FROM hs_ec_trig2 WHERE id = 1"));
            assertEquals("test", str("SELECT data->'name' FROM hs_ec_trig2 WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_trig2 CASCADE");
            exec("DROP FUNCTION IF EXISTS hs_ec_add_key_fn");
        }
    }

    // =========================================================================
    // J. CTEs and subqueries
    // =========================================================================

    @Test
    void cte_with_hstore_filter() throws SQLException {
        exec("CREATE TABLE hs_ec_cte (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_cte VALUES (1, 'type=>fruit'), (2, 'type=>veg'), (3, 'type=>fruit')");
            assertEquals(2, count("WITH fruits AS (SELECT * FROM hs_ec_cte WHERE data->'type' = 'fruit') SELECT count(*) FROM fruits"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_cte");
        }
    }

    @Test
    void correlated_subquery_with_hstore() throws SQLException {
        exec("CREATE TABLE hs_ec_sub (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_sub VALUES (1, 'type=>a'), (2, 'type=>b'), (3, 'type=>a')");
            assertEquals("2", str("SELECT (SELECT count(*) FROM hs_ec_sub t2 WHERE t2.data->'type' = t.data->'type') "
                    + "FROM hs_ec_sub t WHERE t.id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_sub");
        }
    }

    // =========================================================================
    // K. Views over hstore columns
    // =========================================================================

    @Test
    void view_extracting_hstore_keys() throws SQLException {
        exec("CREATE TABLE hs_ec_view (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_ec_view VALUES (1, 'name=>alice, role=>admin'), (2, 'name=>bob, role=>user')");
        exec("CREATE VIEW hs_ec_v_users AS SELECT id, data->'name' AS name, data->'role' AS role FROM hs_ec_view");
        try {
            assertEquals("alice", str("SELECT name FROM hs_ec_v_users WHERE id = 1"));
            assertEquals("admin", str("SELECT role FROM hs_ec_v_users WHERE role = 'admin'"));
            assertEquals(1, count("SELECT count(*) FROM hs_ec_v_users WHERE role = 'admin'"));
        } finally {
            exec("DROP VIEW IF EXISTS hs_ec_v_users");
            exec("DROP TABLE IF EXISTS hs_ec_view");
        }
    }

    // =========================================================================
    // L. PREPARE / EXECUTE with hstore
    // =========================================================================

    @Test
    void prepared_statement_with_hstore() throws SQLException {
        exec("CREATE TABLE hs_ec_prep (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_prep VALUES (1, 'x=>10'), (2, 'x=>20')");
            exec("PREPARE hs_ec_fetch(int) AS SELECT data->'x' AS val FROM hs_ec_prep WHERE id = $1");
            assertEquals("10", str("EXECUTE hs_ec_fetch(1)"));
            assertEquals("20", str("EXECUTE hs_ec_fetch(2)"));
            exec("DEALLOCATE hs_ec_fetch");
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_prep");
        }
    }

    // =========================================================================
    // M. DO blocks with hstore
    // =========================================================================

    @Test
    void do_block_inserts_hstore() throws SQLException {
        exec("CREATE TABLE hs_ec_do (id int PRIMARY KEY, data hstore)");
        try {
            exec("DO $$ BEGIN INSERT INTO hs_ec_do VALUES (1, 'from_do=>true'); END; $$");
            assertEquals("true", str("SELECT data->'from_do' FROM hs_ec_do WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_do");
        }
    }

    @Test
    void do_block_with_hstore_variable() throws SQLException {
        exec("CREATE TABLE hs_ec_do2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("DO $$ DECLARE h hstore; BEGIN h := 'k1=>v1, k2=>v2'::hstore; INSERT INTO hs_ec_do2 VALUES (1, h); END; $$");
            assertEquals("v2", str("SELECT data->'k2' FROM hs_ec_do2 WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_do2");
        }
    }

    @Test
    void do_block_with_hstore_concat() throws SQLException {
        exec("CREATE TABLE hs_ec_do3 (id int PRIMARY KEY, data hstore)");
        try {
            exec("DO $$ DECLARE a hstore := 'x=>1'::hstore; b hstore := 'y=>2'::hstore; BEGIN INSERT INTO hs_ec_do3 VALUES (1, a || b); END; $$");
            assertEquals("1", str("SELECT data->'x' FROM hs_ec_do3 WHERE id = 1"));
            assertEquals("2", str("SELECT data->'y' FROM hs_ec_do3 WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_do3");
        }
    }

    // =========================================================================
    // N. hstore in aggregate expressions
    // =========================================================================

    @Test
    void aggregate_on_hstore_extracted_values() throws SQLException {
        exec("CREATE TABLE hs_ec_agg (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_agg VALUES (1, 'score=>80'), (2, 'score=>90'), (3, 'score=>100')");
            assertEquals("90.0000000000000000", str("SELECT avg((data->'score')::numeric) FROM hs_ec_agg"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_agg");
        }
    }

    @Test
    void count_with_hstore_condition() throws SQLException {
        exec("CREATE TABLE hs_ec_agg2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_agg2 VALUES (1, 'score=>50'), (2, 'score=>90'), (3, 'score=>95')");
            assertEquals(2, count("SELECT count(*) FROM hs_ec_agg2 WHERE (data->'score')::int > 80"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_agg2");
        }
    }

    @Test
    void group_by_hstore_arrow_in_select() throws SQLException {
        exec("CREATE TABLE hs_ec_grp (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_grp VALUES (1, 'score=>85, team=>a'), (2, 'score=>92, team=>b'), (3, 'score=>78, team=>a'), (4, 'score=>95, team=>b')");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT data->'team' AS team, avg((data->'score')::numeric) AS avg_score FROM hs_ec_grp GROUP BY data->'team' ORDER BY data->'team'")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString("team"), "team should be 'a' not null");
                assertTrue(rs.next());
                assertEquals("b", rs.getString("team"), "team should be 'b' not null");
                assertFalse(rs.next());
            }
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_grp");
        }
    }

    // =========================================================================
    // O. hstore with UNION / INTERSECT / EXCEPT
    // =========================================================================

    @Test
    void union_on_hstore_columns() throws SQLException {
        exec("CREATE TABLE hs_ec_u1 (id int PRIMARY KEY, data hstore)");
        exec("CREATE TABLE hs_ec_u2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_u1 VALUES (1, 'a=>1'), (2, 'b=>2')");
            exec("INSERT INTO hs_ec_u2 VALUES (3, 'b=>2'), (4, 'c=>3')");
            // UNION deduplicates: a=>1, b=>2, c=>3 = 3 distinct
            assertEquals(3, count("SELECT count(*) FROM (SELECT data FROM hs_ec_u1 UNION SELECT data FROM hs_ec_u2) sub"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_u1");
            exec("DROP TABLE IF EXISTS hs_ec_u2");
        }
    }

    @Test
    void intersect_on_hstore_columns() throws SQLException {
        exec("CREATE TABLE hs_ec_i1 (id int PRIMARY KEY, data hstore)");
        exec("CREATE TABLE hs_ec_i2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_i1 VALUES (1, 'a=>1'), (2, 'b=>2')");
            exec("INSERT INTO hs_ec_i2 VALUES (3, 'b=>2'), (4, 'c=>3')");
            // b=>2 is in both
            assertEquals(1, count("SELECT count(*) FROM (SELECT data FROM hs_ec_i1 INTERSECT SELECT data FROM hs_ec_i2) sub"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_i1");
            exec("DROP TABLE IF EXISTS hs_ec_i2");
        }
    }

    @Test
    void except_on_hstore_columns() throws SQLException {
        exec("CREATE TABLE hs_ec_e1 (id int PRIMARY KEY, data hstore)");
        exec("CREATE TABLE hs_ec_e2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_ec_e1 VALUES (1, 'a=>1'), (2, 'b=>2')");
            exec("INSERT INTO hs_ec_e2 VALUES (3, 'b=>2'), (4, 'c=>3')");
            // a=>1 is only in e1
            assertEquals(1, count("SELECT count(*) FROM (SELECT data FROM hs_ec_e1 EXCEPT SELECT data FROM hs_ec_e2) sub"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_ec_e1");
            exec("DROP TABLE IF EXISTS hs_ec_e2");
        }
    }

    // =========================================================================
    // P. Coverage probes — previously untested corner cases
    // =========================================================================

    @Test
    void akeys_empty_hstore_returns_empty_array() throws SQLException {
        assertEquals("{}", str("SELECT akeys(''::hstore)::text"));
    }

    @Test
    void avals_empty_hstore_returns_empty_array() throws SQLException {
        assertEquals("{}", str("SELECT avals(''::hstore)::text"));
    }

    @Test
    void window_function_cumulative_sum_over_hstore_arrow() throws SQLException {
        exec("CREATE TABLE hs_probe_win (id serial primary key, data hstore)");
        exec("INSERT INTO hs_probe_win (data) VALUES ('score=>10, team=>a'), ('score=>20, team=>a'), ('score=>30, team=>b')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT id, sum((data->'score')::int) OVER (PARTITION BY data->'team' ORDER BY id) as cum_sum "
               + "FROM hs_probe_win ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals(10, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals(30, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals(30, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            exec("DROP TABLE hs_probe_win");
        }
    }
}
