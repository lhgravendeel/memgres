package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 5 failures from not-enforced-constraints.sql where Memgres
 * diverges from PostgreSQL 18 behavior.
 *
 * Stmt 33  - SELECT * FROM ne_toggle should return 0 rows after toggle cycle
 * Stmt 72  - UNIQUE NOT ENFORCED should error with SQLSTATE 0A000
 * Stmt 75  - PRIMARY KEY NOT ENFORCED should error with SQLSTATE 0A000
 * Stmt 113 - ALTER CONSTRAINT ... ENFORCED on violating data should error with SQLSTATE 42809
 * Stmt 135 - information_schema.table_constraints.enforced should report NO for NOT ENFORCED constraint
 */
class NotEnforcedConstraintsCompat15Test {

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
            s.execute("DROP SCHEMA IF EXISTS ne_test CASCADE");
            s.execute("CREATE SCHEMA ne_test");
            s.execute("SET search_path = ne_test, public");

            // --- Setup for Stmt 33 (ne_toggle table) ---
            // Section 9: ALTER TABLE ALTER CONSTRAINT ... NOT ENFORCED
            s.execute("CREATE TABLE ne_toggle ("
                    + "  id integer PRIMARY KEY,"
                    + "  val integer,"
                    + "  CONSTRAINT chk_toggle CHECK (val > 0)"
                    + ")");

            // Enforced: insert of negative value should fail
            try {
                s.execute("INSERT INTO ne_toggle VALUES (1, -5)");
            } catch (SQLException ignored) {
                // Expected: violates check constraint
            }

            // Disable enforcement
            s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED");

            // Now accepts negative
            s.execute("INSERT INTO ne_toggle VALUES (1, -5)");

            // --- Setup for Stmt 113 (ne_toggle_validate table) ---
            // Section 31: toggling ENFORCED validates existing data
            s.execute("CREATE TABLE ne_toggle_validate ("
                    + "  id integer PRIMARY KEY,"
                    + "  val integer,"
                    + "  CONSTRAINT chk_toggle CHECK (val > 0) NOT ENFORCED"
                    + ")");
            s.execute("INSERT INTO ne_toggle_validate VALUES (1, -5)");

            // --- Setup for Stmt 135 (ne_info_toggle table) ---
            // Section 35: information_schema after toggling enforcement
            s.execute("CREATE TABLE ne_info_toggle ("
                    + "  id integer PRIMARY KEY,"
                    + "  val integer,"
                    + "  CONSTRAINT chk_info CHECK (val > 0) NOT ENFORCED"
                    + ")");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ne_test CASCADE");
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

    private int queryRowCount(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }

    /**
     * Stmt 33: After creating ne_toggle with CHECK (val > 0) ENFORCED, failing to
     * insert a negative value, toggling to NOT ENFORCED and inserting a negative value,
     * SELECT * FROM ne_toggle should return 1 row.
     *
     * The row was successfully inserted after constraint was made NOT ENFORCED.
     */
    @Test
    void testToggleConstraintSelectReturnsOneRow() throws SQLException {
        int rowCount = queryRowCount("SELECT * FROM ne_toggle");
        assertEquals(1, rowCount,
                "SELECT * FROM ne_toggle should return 1 row (inserted after NOT ENFORCED toggle)");
    }

    /**
     * Stmt 72: CREATE TABLE with UNIQUE constraint marked NOT ENFORCED should fail
     * with SQLSTATE 0A000 (feature_not_supported) because PG 18 does not allow
     * UNIQUE constraints to be NOT ENFORCED.
     *
     * PG: ERROR [0A000] UNIQUE constraints cannot be marked NOT ENFORCED
     * Memgres: ERROR [42601] parse error
     */
    @Test
    void testUniqueNotEnforcedErrorsSqlstate0A000() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE ne_unique ("
                        + "  id integer PRIMARY KEY,"
                        + "  code text,"
                        + "  CONSTRAINT uq_code UNIQUE (code) NOT ENFORCED"
                        + ")")
        );
        assertEquals("0A000", ex.getSQLState(),
                "UNIQUE NOT ENFORCED should produce SQLSTATE 0A000 (feature_not_supported)");
        assertTrue(ex.getMessage().toLowerCase().contains("not enforced"),
                "Error message should mention NOT ENFORCED, got: " + ex.getMessage());
    }

    /**
     * Stmt 75: CREATE TABLE with PRIMARY KEY constraint marked NOT ENFORCED should
     * fail with SQLSTATE 0A000 (feature_not_supported) because PG 18 does not allow
     * PRIMARY KEY constraints to be NOT ENFORCED.
     *
     * PG: ERROR [0A000] PRIMARY KEY constraints cannot be marked NOT ENFORCED
     * Memgres: ERROR [42601] parse error
     */
    @Test
    void testPrimaryKeyNotEnforcedErrorsSqlstate0A000() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE ne_pk_fail ("
                        + "  id integer,"
                        + "  CONSTRAINT pk_fail PRIMARY KEY (id) NOT ENFORCED"
                        + ")")
        );
        assertEquals("0A000", ex.getSQLState(),
                "PRIMARY KEY NOT ENFORCED should produce SQLSTATE 0A000 (feature_not_supported)");
        assertTrue(ex.getMessage().toLowerCase().contains("primary key"),
                "Error message should mention 'primary key', got: " + ex.getMessage());
    }

    /**
     * Stmt 113: ALTER TABLE ne_toggle_validate ALTER CONSTRAINT chk_toggle ENFORCED
     * should fail with SQLSTATE 42809 because existing data (val = -5) violates the
     * constraint, so enforcement cannot be toggled on.
     *
     * PG: ERROR [42809] cannot alter enforceability of constraint "chk_toggle"
     * Memgres: OK (incorrectly succeeds)
     */
    @Test
    void testAlterConstraintEnforcedOnViolatingDataErrors() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE ne_toggle_validate ALTER CONSTRAINT chk_toggle ENFORCED")
        );
        assertEquals("42809", ex.getSQLState(),
                "ALTER CONSTRAINT ... ENFORCED with violating data should produce SQLSTATE 42809");
        assertTrue(ex.getMessage().toLowerCase().contains("cannot alter enforceability"),
                "Error message should mention 'cannot alter enforceability', got: " + ex.getMessage());
    }

    /**
     * Stmt 135: The information_schema.table_constraints.enforced column should
     * report 'NO' for a NOT ENFORCED constraint.
     *
     * PG: [NO]
     * Memgres: [YES]
     */
    @Test
    void testInformationSchemaEnforcedReportsNoForNotEnforced() throws SQLException {
        String enforced = query1(
                "SELECT enforced FROM information_schema.table_constraints "
                        + "WHERE constraint_name = 'chk_info' AND table_schema = 'ne_test'");
        assertEquals("NO", enforced,
                "information_schema.table_constraints.enforced should be 'NO' for a NOT ENFORCED constraint");
    }
}
