package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 9: Remaining validation gaps. Misc cases where memgres accepts
 * what PG rejects, or vice versa. Covers: EXPLAIN/COPY cascading, COMMENT ON
 * edge cases, SET ROLE, DROP ROLE, partition validation, temp table behavior,
 * function overload detection, procedure/call, setval on missing seq, and more.
 */
class RemainingValidationTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void assertSqlFails(String sql) {
        assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
        }, "Expected error for: " + sql);
    }

    static void assertSqlState(String sql, String expected) {
        try {
            exec(sql);
            fail("Expected error " + expected + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(expected, e.getSQLState(), "Wrong SQLSTATE for: " + sql + ", got: " + e.getMessage());
        }
    }

    // ========================================================================
    // EXPLAIN with valid options should succeed
    // ========================================================================

    @Test
    void explain_analyze_false_costs_false_succeeds() throws SQLException {
        exec("CREATE TABLE rv_explain (id INT)");
        try {
            // PG accepts these; they're valid EXPLAIN options
            exec("EXPLAIN (ANALYZE false, COSTS false, VERBOSE true) SELECT * FROM rv_explain");
            exec("EXPLAIN (FORMAT text) SELECT * FROM rv_explain");
            exec("EXPLAIN (FORMAT json) SELECT * FROM rv_explain");
        } finally {
            exec("DROP TABLE rv_explain");
        }
    }

    @Test
    void explain_bad_format_rejected() throws SQLException {
        exec("CREATE TABLE rv_explain2 (id INT)");
        try {
            assertSqlFails("EXPLAIN (FORMAT yamlish) SELECT * FROM rv_explain2");
        } finally {
            exec("DROP TABLE rv_explain2");
        }
    }

    // ========================================================================
    // COMMENT ON VIEW / INDEX: should succeed when object exists
    // ========================================================================

    @Test
    void comment_on_view_succeeds() throws SQLException {
        exec("CREATE TABLE rv_base (id INT)");
        exec("CREATE VIEW rv_v AS SELECT * FROM rv_base");
        try {
            exec("COMMENT ON VIEW rv_v IS 'view comment'");
            // Should not error
        } finally {
            exec("DROP VIEW rv_v");
            exec("DROP TABLE rv_base");
        }
    }

    @Test
    void comment_on_index_succeeds() throws SQLException {
        exec("CREATE TABLE rv_idx_t (id INT PRIMARY KEY)");
        exec("CREATE INDEX rv_idx_t_note_idx ON rv_idx_t (id)");
        try {
            // COMMENT ON INDEX works for explicitly created indexes.
            // PG18 also allows COMMENT ON INDEX for PK constraint-backed indexes.
            exec("COMMENT ON INDEX rv_idx_t_note_idx IS 'explicit index comment'");
        } finally {
            exec("DROP TABLE rv_idx_t");
        }
    }

    // ========================================================================
    // SET ROLE / DROP ROLE validation
    // ========================================================================

    @Test
    void set_role_nonexistent_rejected() {
        // SET ROLE to a role that doesn't exist should fail
        assertSqlFails("SET ROLE no_such_role_rv");
    }

    @Test
    void drop_role_nonexistent_rejected() {
        assertSqlFails("DROP ROLE no_such_role_rv");
    }

    @Test
    void set_role_none_succeeds() throws SQLException {
        // SET ROLE NONE always succeeds; resets to session user
        exec("SET ROLE NONE");
    }

    // ========================================================================
    // CREATE TRIGGER: function return type validation
    // ========================================================================

    @Test
    void create_trigger_function_wrong_return_type_rejected() throws SQLException {
        exec("CREATE TABLE rv_trig (id INT)");
        exec("CREATE FUNCTION rv_bad_fn() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$");
        try {
            // Trigger function must return TRIGGER, not INT
            assertSqlFails(
                "CREATE TRIGGER rv_bad AFTER INSERT ON rv_trig FOR EACH ROW EXECUTE FUNCTION rv_bad_fn()");
        } finally {
            exec("DROP FUNCTION IF EXISTS rv_bad_fn()");
            exec("DROP TABLE rv_trig CASCADE");
        }
    }

    // ========================================================================
    // CREATE RULE on nonexistent table
    // ========================================================================

    @Test
    void create_rule_on_nonexistent_table_rejected() {
        assertSqlFails("CREATE RULE rv_rule AS ON INSERT TO no_such_table DO INSTEAD NOTHING");
    }

    // ========================================================================
    // PL/pgSQL function with invalid body
    // ========================================================================

    @Test
    void create_plpgsql_function_bad_body_rejected() {
        // BEGIN RETURN; END, missing RETURN value for non-void function
        assertSqlFails(
            "CREATE FUNCTION rv_badpl() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$");
    }

    // ========================================================================
    // Function overload: same name, same arg types should fail
    // ========================================================================

    @Test
    void create_function_duplicate_rejected() throws SQLException {
        exec("CREATE FUNCTION rv_f(a INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a $$");
        try {
            // PG rejects: function rv_f(integer) already exists
            assertSqlFails("CREATE FUNCTION rv_f(a INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a * 2 $$");
        } finally {
            exec("DROP FUNCTION IF EXISTS rv_f(INT)");
        }
    }

    // ========================================================================
    // PROCEDURE + CALL
    // ========================================================================

    @Test
    void create_procedure_and_call() throws SQLException {
        exec("CREATE TABLE rv_proc_t (id INT, val TEXT)");
        try {
            // SQL procedures should be supported
            exec("CREATE PROCEDURE rv_proc(i INT, t TEXT) LANGUAGE SQL AS $$ INSERT INTO rv_proc_t VALUES (i, t) $$");
            exec("CALL rv_proc(1, 'hello')");
            assertEquals("hello", q("SELECT val FROM rv_proc_t WHERE id = 1"));
            exec("DROP PROCEDURE rv_proc");
        } finally {
            exec("DROP TABLE rv_proc_t");
        }
    }

    // ========================================================================
    // setval on nonexistent sequence
    // ========================================================================

    @Test
    void setval_nonexistent_seq_rejected() {
        assertSqlFails("SELECT setval('no_such_seq_rv', 100)");
    }

    // ========================================================================
    // xmlserialize to wrong type
    // ========================================================================

    @Test
    void xmlserialize_to_int_rejected() {
        assertSqlFails("SELECT xmlserialize(document xmlelement(name x, 'abc') AS int)");
    }

    // ========================================================================
    // to_tsquery with invalid syntax
    // ========================================================================

    @Test
    void tsquery_invalid_syntax_rejected() {
        // 'quick & & fox': double & is invalid
        assertSqlFails("SELECT to_tsquery('english', 'quick & & fox')");
    }

    @Test
    void tsvector_invalid_config_rejected() {
        assertSqlFails("SELECT to_tsvector('no_such_config', 'abc')");
    }

    // ========================================================================
    // IDENTITY ALWAYS: reject explicit value without OVERRIDING
    // ========================================================================

    @Test
    void identity_always_rejects_explicit_value() throws SQLException {
        exec("CREATE TABLE rv_ident (id INT GENERATED ALWAYS AS IDENTITY, val TEXT)");
        try {
            assertSqlState("INSERT INTO rv_ident(id, val) VALUES (100, 'bad')", "428C9");
        } finally {
            exec("DROP TABLE rv_ident");
        }
    }

    // ========================================================================
    // Updatable view: constraint check propagation
    // ========================================================================

    @Test
    void updatable_view_check_constraint_propagated() throws SQLException {
        exec("CREATE TABLE rv_ck_base (id SERIAL PRIMARY KEY, a INT CHECK (a > 0), b TEXT)");
        exec("CREATE VIEW rv_ck_v AS SELECT id, a, b FROM rv_ck_base");
        try {
            exec("INSERT INTO rv_ck_v (a, b) VALUES (5, 'ok')"); // should succeed
            // Negative value should fail the CHECK constraint
            assertSqlFails("INSERT INTO rv_ck_v (a, b) VALUES (-1, 'bad')");
        } finally {
            exec("DROP VIEW rv_ck_v");
            exec("DROP TABLE rv_ck_base");
        }
    }

    // ========================================================================
    // Temp table ON COMMIT DELETE ROWS
    // ========================================================================

    @Test
    void temp_table_on_commit_delete_rows() throws SQLException {
        exec("BEGIN");
        exec("CREATE TEMP TABLE rv_tt_del (a INT) ON COMMIT DELETE ROWS");
        exec("INSERT INTO rv_tt_del VALUES (1), (2)");
        exec("COMMIT");
        // Table exists but rows are deleted
        assertEquals("0", q("SELECT COUNT(*) FROM rv_tt_del"));
    }

    // ========================================================================
    // ALTER TABLE RENAME column: only fails when view depends
    // ========================================================================

    @Test
    void alter_rename_column_no_view_succeeds() throws SQLException {
        exec("CREATE TABLE rv_ren (a INT, b TEXT)");
        try {
            exec("ALTER TABLE rv_ren RENAME a TO aa");
            // Should succeed when no view depends on the column
        } finally {
            exec("DROP TABLE rv_ren");
        }
    }

    @Test
    void alter_rename_column_with_view_should_succeed() throws SQLException {
        // PG allows RENAME even with view dependency (view silently updates)
        exec("CREATE TABLE rv_ren2 (a INT, b TEXT)");
        exec("CREATE VIEW rv_ren2_v AS SELECT * FROM rv_ren2");
        try {
            exec("ALTER TABLE rv_ren2 RENAME COLUMN a TO aa");
            // PG allows this; the view updates to reference the new column name
        } finally {
            exec("DROP VIEW IF EXISTS rv_ren2_v");
            exec("DROP TABLE rv_ren2");
        }
    }

    // ========================================================================
    // CREATE FUNCTION with DEFAULT keyword in param (invalid)
    // ========================================================================

    @Test
    void function_param_default_without_value_rejected() {
        // 'a int DEFAULT' without a default value is invalid syntax
        assertSqlFails("CREATE FUNCTION rv_bad_default(a int DEFAULT) RETURNS int LANGUAGE SQL AS $$ SELECT 1 $$");
    }

    // ========================================================================
    // FOR UPDATE OF nonexistent table
    // ========================================================================

    @Test
    void for_update_of_nonexistent_table_rejected() throws SQLException {
        exec("CREATE TABLE rv_lock_t (id INT)");
        exec("INSERT INTO rv_lock_t VALUES (1)");
        try {
            assertSqlFails("SELECT * FROM rv_lock_t ORDER BY id FOR UPDATE OF no_such_table");
        } finally {
            exec("DROP TABLE rv_lock_t");
        }
    }

    // ========================================================================
    // Custom GUC parameter: PG allows schema-prefixed GUC names
    // ========================================================================

    @Test
    void set_custom_guc_with_schema_prefix_succeeds() throws SQLException {
        // PG allows SET schema.param = 'value' for custom GUC parameters
        exec("SET myapp.tenant_id = '10'");
        assertEquals("10", q("SHOW myapp.tenant_id"));
    }

    // ========================================================================
    // NaN::numeric
    // ========================================================================

    @Test
    void nan_numeric_cast_succeeds() throws SQLException {
        String result = q("SELECT 'NaN'::numeric");
        assertNotNull(result);
        assertEquals("NaN", result);
    }

    // ========================================================================
    // array_append type mismatch
    // ========================================================================

    @Test
    void array_append_type_mismatch_rejected() {
        assertSqlFails("SELECT array_append(ARRAY[1,2], 'x')");
    }

    // ========================================================================
    // format positional args: accessing beyond arg count
    // ========================================================================

    @Test
    void format_positional_beyond_count_rejected() {
        // %3$s but only 2 args provided
        assertSqlFails("SELECT format('%2$s %1$s %3$s', 'a', 'b')");
    }
}
