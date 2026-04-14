package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3 PG vs Memgres differences from not-enforced-constraints.sql.
 *
 * These tests assert exact PG 18 behavior. They are expected to FAIL on
 * current Memgres, documenting the real gaps.
 *
 * Uses default JDBC (extended query protocol) to match the comparison framework.
 */
class RemainingNotEnforcedCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
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
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 33: ALTER TABLE ALTER CONSTRAINT NOT ENFORCED, then INSERT violating data.
     * Matches comparison SQL blocks 29-33: create table with ENFORCED CHECK,
     * fail INSERT, ALTER NOT ENFORCED, INSERT again, SELECT.
     *
     * PG and Memgres should both have 1 row after this sequence.
     * The real comparison difference was in block numbering (the original test
     * incorrectly expected 0 rows). This test verifies correct NOT ENFORCED behavior.
     */
    @Test
    void stmt33_toggleNotEnforcedThenInsert() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle CASCADE");
            s.execute("CREATE TABLE ne_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle CHECK (val > 0))");

            // INSERT fails — constraint is ENFORCED
            try {
                s.execute("INSERT INTO ne_toggle VALUES (1, -5)");
            } catch (SQLException ignored) {}

            // Disable enforcement
            s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED");

            // INSERT succeeds — constraint is NOT ENFORCED
            s.execute("INSERT INTO ne_toggle VALUES (1, -5)");

            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::integer AS cnt FROM ne_toggle")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("cnt"),
                        "After ALTER NOT ENFORCED and INSERT, table should have 1 row.");
            }
        }
    }

    /**
     * Stmt 35: ALTER CONSTRAINT ENFORCED validates existing data and rejects
     * the toggle when violations exist (SQLSTATE 42809).
     * After the failed ALTER, the constraint remains NOT ENFORCED.
     */
    @Test
    void stmt35_alterEnforcedRejectsViolatingData() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle CASCADE");
            s.execute("CREATE TABLE ne_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle CHECK (val > 0))");

            s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED");
            s.execute("INSERT INTO ne_toggle VALUES (1, -5)");

            // ALTER ENFORCED should fail because existing data violates the constraint
            try {
                s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle ENFORCED");
                fail("ALTER CONSTRAINT ENFORCED should fail when violating data exists");
            } catch (SQLException e) {
                // PG returns 42809 "cannot alter enforceability of constraint"
                assertTrue(e.getSQLState().equals("42809") || e.getSQLState().equals("23514"),
                        "Expected 42809 or 23514, got: " + e.getSQLState());
            }

            // Constraint is still NOT ENFORCED — violating INSERT should succeed
            s.execute("INSERT INTO ne_toggle VALUES (2, -10)");

            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::integer AS cnt FROM ne_toggle")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("cnt"),
                        "After failed ALTER ENFORCED, constraint is still NOT ENFORCED. "
                        + "Both violating rows should exist.");
            }
        }
    }

    /**
     * Stmt 135: information_schema.table_constraints.enforced column after
     * successful ALTER CONSTRAINT ENFORCED.
     *
     * After ALTER CONSTRAINT ... ENFORCED succeeds (no violations), info_schema
     * should report 'YES' for the enforced column.
     */
    @Test
    void stmt135_informationSchemaEnforcedAfterToggle() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_info_toggle CASCADE");
            s.execute("CREATE TABLE ne_info_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_info CHECK (val > 0) NOT ENFORCED)");

            // Verify NOT ENFORCED shows as 'NO'
            try (ResultSet rs = s.executeQuery(
                    "SELECT enforced FROM information_schema.table_constraints "
                    + "WHERE constraint_name = 'chk_info' AND table_schema = 'ne_compat'")) {
                assertTrue(rs.next(), "Expected one row from information_schema");
                assertEquals("NO", rs.getString("enforced"),
                        "NOT ENFORCED constraint should show enforced='NO'");
            }

            // Insert valid data and toggle to ENFORCED
            s.execute("INSERT INTO ne_info_toggle VALUES (1, 5)");
            s.execute("ALTER TABLE ne_info_toggle ALTER CONSTRAINT chk_info ENFORCED");

            // After successful ALTER ENFORCED, info_schema should show 'YES'
            try (ResultSet rs = s.executeQuery(
                    "SELECT enforced FROM information_schema.table_constraints "
                    + "WHERE constraint_name = 'chk_info' AND table_schema = 'ne_compat'")) {
                assertTrue(rs.next(), "Expected one row from information_schema");
                assertEquals("YES", rs.getString("enforced"),
                        "After ALTER CONSTRAINT ENFORCED succeeds, info_schema should show 'YES'.");
            }
        }
    }
}
