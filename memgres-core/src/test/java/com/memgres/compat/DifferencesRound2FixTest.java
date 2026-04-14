package com.memgres.compat;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for round-2 differences fixes from the feature-comparison report.
 *
 * S1: ALTER FUNCTION IF EXISTS ... RENAME should be rejected (PG 18 syntax error)
 * S2: ALTER FUNCTION pg_sleep(double precision) should find pg_sleep in catalog
 * S3: ALTER INDEX ... SET SCHEMA should be rejected (PG 18 syntax error)
 * S4: CREATE LANGUAGE IF NOT EXISTS should be rejected (PG 18 syntax error)
 * S7: CREATE INDEX with JSON_VALUE expression should succeed
 */
class DifferencesRound2FixTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS dr2_test CASCADE");
            s.execute("CREATE SCHEMA dr2_test");
            s.execute("SET search_path = dr2_test, public");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS dr2_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    // ========================================================================
    // S1: ALTER FUNCTION IF EXISTS ... RENAME should error
    // PG 18 does not support IF EXISTS on ALTER FUNCTION ... RENAME TO
    // ========================================================================

    @Test
    void alterFunctionIfExistsRenameRejected() {
        // PG rejects with 42601: syntax error at or near "EXISTS"
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER FUNCTION IF EXISTS dr2_nonexistent_xyz() RENAME TO dr2_something"));
        assertEquals("42601", ex.getSQLState(),
                "ALTER FUNCTION IF EXISTS ... RENAME should be rejected with syntax error");
    }

    @Test
    void alterFunctionRenameWithoutIfExistsStillWorks() throws SQLException {
        exec("CREATE FUNCTION dr2_rename_me() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$");
        exec("ALTER FUNCTION dr2_rename_me() RENAME TO dr2_renamed");
        // Verify the rename worked
        String result = query1("SELECT dr2_renamed()");
        assertEquals("1", result);
    }

    // ========================================================================
    // S2: ALTER FUNCTION pg_sleep(double precision) IMMUTABLE should succeed
    // PG has pg_sleep in pg_proc; Memgres should too
    // ========================================================================

    @Test
    void alterFunctionPgSleepImmutableSucceeds() {
        // PG allows altering built-in functions when run as superuser
        assertDoesNotThrow(() ->
                exec("ALTER FUNCTION pg_sleep(double precision) IMMUTABLE"));
    }

    // ========================================================================
    // S3: ALTER INDEX ... SET SCHEMA should error
    // PG 18 does not support SET SCHEMA on ALTER INDEX
    // ========================================================================

    @Test
    void alterIndexSetSchemaRejected() throws SQLException {
        exec("CREATE TABLE dr2_idx_tbl (id integer PRIMARY KEY, val integer)");
        exec("CREATE INDEX dr2_idx_move ON dr2_idx_tbl (val)");
        exec("CREATE SCHEMA IF NOT EXISTS dr2_other");

        // PG rejects with 42601: syntax error at or near "SCHEMA"
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER INDEX dr2_idx_move SET SCHEMA dr2_other"));
        assertEquals("42601", ex.getSQLState(),
                "ALTER INDEX SET SCHEMA should be rejected with syntax error");
    }

    @Test
    void alterIndexRenameStillWorks() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS dr2_idx_tbl2 (id integer PRIMARY KEY, val integer)");
        exec("CREATE INDEX dr2_idx_rename ON dr2_idx_tbl2 (val)");
        exec("ALTER INDEX dr2_idx_rename RENAME TO dr2_idx_renamed");
        // Verify rename worked
        String exists = query1(
                "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = 'dr2_idx_renamed' AND relkind = 'i')::text");
        assertEquals("true", exists);
    }

    // ========================================================================
    // S4: CREATE LANGUAGE IF NOT EXISTS should error
    // PG 18 does not support IF NOT EXISTS on CREATE LANGUAGE
    // ========================================================================

    @Test
    void createLanguageIfNotExistsRejected() {
        // PG rejects with 42601: syntax error at or near "NOT"
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE LANGUAGE IF NOT EXISTS plpgsql"));
        assertEquals("42601", ex.getSQLState(),
                "CREATE LANGUAGE IF NOT EXISTS should be rejected with syntax error");
    }

    @Test
    void createLanguageWithoutIfNotExistsStillWorks() {
        // Plain CREATE LANGUAGE should still be accepted (no-op)
        assertDoesNotThrow(() -> exec("CREATE LANGUAGE plpgsql"));
    }

    // ========================================================================
    // S7: CREATE INDEX with JSON_VALUE expression should succeed
    // PG allows JSON_VALUE in expression indexes
    // ========================================================================

    @Test
    void createIndexWithJsonValueSucceeds() throws SQLException {
        exec("CREATE TABLE dr2_json_idx (id integer PRIMARY KEY, doc jsonb)");
        exec("INSERT INTO dr2_json_idx VALUES (1, '{\"name\": \"Alice\", \"score\": 90}')");
        exec("INSERT INTO dr2_json_idx VALUES (2, '{\"name\": \"Bob\", \"score\": 75}')");

        // PG succeeds; Memgres currently errors with "function json_value(text) does not exist"
        assertDoesNotThrow(() ->
                exec("CREATE INDEX dr2_idx_jv ON dr2_json_idx ((JSON_VALUE(doc, '$.name' RETURNING text)))"));
    }

    @Test
    void queryUsingJsonValueIndexSucceeds() throws SQLException {
        // Verify the index can be used (query should work after index creation)
        exec("CREATE TABLE IF NOT EXISTS dr2_json_q (id integer PRIMARY KEY, doc jsonb)");
        exec("INSERT INTO dr2_json_q VALUES (1, '{\"name\": \"Alice\"}')");
        exec("INSERT INTO dr2_json_q VALUES (2, '{\"name\": \"Bob\"}')");
        exec("CREATE INDEX dr2_idx_jv_q ON dr2_json_q ((JSON_VALUE(doc, '$.name' RETURNING text)))");

        String name = query1("SELECT JSON_VALUE(doc, '$.name' RETURNING text) FROM dr2_json_q WHERE id = 2");
        assertEquals("Bob", name);
    }
}
