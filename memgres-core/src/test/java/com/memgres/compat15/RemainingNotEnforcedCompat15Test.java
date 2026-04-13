package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3 remaining Memgres-vs-PG differences from not-enforced-constraints.sql.
 *
 * These tests assert PG 18 behavior. They are expected to FAIL on current
 * Memgres and pass once the underlying issues are fixed.
 */
class RemainingNotEnforcedCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS ne_compat CASCADE");
            s.execute("CREATE SCHEMA ne_compat");
            s.execute("SET search_path = ne_compat, public");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ne_compat CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 33: Toggling NOT ENFORCED -> ENFORCED with violating data.
    // PG validates existing data and rejects the ALTER when violations exist.
    // Memgres: also rejects (42809), consistent with PG behavior.
    // After rejection, constraint remains NOT ENFORCED and violating row stays.
    // ========================================================================
    @Test
    void stmt33_toggleEnforcedRejectsWithViolatingData() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle CASCADE");
            s.execute("CREATE TABLE ne_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle CHECK (val > 0))");

            s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED");
            s.execute("INSERT INTO ne_toggle VALUES (1, -5)");

            // Re-enabling ENFORCED should fail because of violating data
            try {
                s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle ENFORCED");
                fail("Expected error when re-enabling enforcement with violating data");
            } catch (SQLException e) {
                // PG rejects with an error when validation finds violations
                assertTrue(e.getSQLState() != null,
                        "Should get a SQLSTATE error for constraint violation");
            }

            // Violating row should still be there (ALTER failed, constraint still NOT ENFORCED)
            try (ResultSet rs = s.executeQuery("SELECT count(*)::integer AS cnt FROM ne_toggle")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("cnt"),
                        "Row should remain since ALTER CONSTRAINT ENFORCED was rejected");
            }
        }
    }

    // ========================================================================
    // Stmt 35: After ENFORCED is re-enabled (without violations), INSERT of
    // violating row should fail with SQLSTATE 23514.
    // ========================================================================
    @Test
    void stmt35_enforcedConstraintShouldRejectViolatingInsert() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle2 CASCADE");
            s.execute("CREATE TABLE ne_toggle2 (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle2 CHECK (val > 0))");

            // Disable and re-enable without any violating data
            s.execute("ALTER TABLE ne_toggle2 ALTER CONSTRAINT chk_toggle2 NOT ENFORCED");
            s.execute("ALTER TABLE ne_toggle2 ALTER CONSTRAINT chk_toggle2 ENFORCED");

            // Now INSERT of a violating row should fail
            try {
                s.execute("INSERT INTO ne_toggle2 VALUES (2, -10)");
                fail("Expected check constraint violation (23514), but INSERT succeeded");
            } catch (SQLException e) {
                assertEquals("23514", e.getSQLState(),
                        "SQLSTATE should be 23514 (check_violation), got: "
                        + e.getSQLState() + " - " + e.getMessage());
            }
        }
    }

    // ========================================================================
    // Stmt 135: information_schema.table_constraints.enforced should be 'NO'
    // for a NOT ENFORCED constraint.
    // PG: [NO]
    // Memgres: [YES]
    // ========================================================================
    @Test
    void stmt135_informationSchemaEnforcedShouldBeNo() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_info_test CASCADE");
            s.execute("CREATE TABLE ne_info_test (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_info_compat CHECK (val > 0) NOT ENFORCED)");

            try (ResultSet rs = s.executeQuery(
                    "SELECT enforced FROM information_schema.table_constraints "
                    + "WHERE constraint_name = 'chk_info_compat' AND table_schema = 'ne_compat'")) {
                assertTrue(rs.next(), "Expected one row from information_schema");
                assertEquals("NO", rs.getString("enforced"),
                        "information_schema.table_constraints.enforced should be 'NO' "
                        + "for a NOT ENFORCED constraint");
            }
        }
    }
}
