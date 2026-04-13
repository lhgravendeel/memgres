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
     * Stmt 33: After creating a NOT ENFORCED constraint, inserting a violating row,
     * then toggling to ENFORCED — PG validates existing data.
     *
     * PG: SELECT * FROM ne_toggle → 0 rows
     * Memgres: SELECT * FROM ne_toggle → 1 row (1, -5)
     */
    @Test
    void stmt33_toggleEnforcedWithViolatingData_pgShows0Rows() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle CASCADE");
            s.execute("CREATE TABLE ne_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle CHECK (val > 0))");

            s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED");
            s.execute("INSERT INTO ne_toggle VALUES (1, -5)");

            try {
                s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle ENFORCED");
            } catch (SQLException ignored) {
            }

            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::integer AS cnt FROM ne_toggle")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt("cnt"),
                        "PG shows 0 rows after the toggle sequence, but Memgres shows rows.");
            }
        }
    }

    /**
     * Stmt 35: INSERT INTO ne_toggle VALUES (2, -10) after the toggle sequence.
     *
     * PG: ERROR [23514] new row violates check constraint "chk_toggle"
     * Memgres: OK 1 rows affected
     */
    @Test
    void stmt35_insertAfterToggle_pgRejects() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle CASCADE");
            s.execute("CREATE TABLE ne_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle CHECK (val > 0))");

            s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED");
            s.execute("INSERT INTO ne_toggle VALUES (1, -5)");

            try {
                s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle ENFORCED");
            } catch (SQLException ignored) {
            }

            try {
                s.execute("INSERT INTO ne_toggle VALUES (2, -10)");
                fail("PG rejects INSERT of violating data with SQLSTATE 23514, "
                        + "but Memgres allowed it");
            } catch (SQLException e) {
                assertEquals("23514", e.getSQLState(),
                        "SQLSTATE should be 23514 (check_violation), got: "
                        + e.getSQLState() + " - " + e.getMessage());
            }
        }
    }

    /**
     * Stmt 135: information_schema.table_constraints.enforced column.
     *
     * The comparison creates a NOT ENFORCED constraint, then ALTERs it to ENFORCED
     * (which should succeed since no violating data). Then it queries info_schema.
     *
     * BUT the comparison's expected result is 'NO' and PG returns 'NO'.
     * This means after ALTER CONSTRAINT chk_info ENFORCED, PG's info_schema
     * still shows 'NO'. Memgres shows 'YES'.
     *
     * To reproduce the exact comparison scenario: create NOT ENFORCED, insert
     * valid data, ALTER to ENFORCED, then query.
     */
    @Test
    void stmt135_informationSchemaEnforcedAfterToggle() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_info_toggle CASCADE");
            s.execute("CREATE TABLE ne_info_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_info CHECK (val > 0) NOT ENFORCED)");

            // Insert valid data and toggle to ENFORCED — matching comparison flow
            s.execute("INSERT INTO ne_info_toggle VALUES (1, 5)");
            s.execute("ALTER TABLE ne_info_toggle ALTER CONSTRAINT chk_info ENFORCED");

            // The comparison expects 'NO' here — PG returns 'NO'
            // Memgres returns 'YES'
            try (ResultSet rs = s.executeQuery(
                    "SELECT enforced FROM information_schema.table_constraints "
                    + "WHERE constraint_name = 'chk_info' AND table_schema = 'ne_compat'")) {
                assertTrue(rs.next(), "Expected one row from information_schema");
                assertEquals("NO", rs.getString("enforced"),
                        "PG comparison shows enforced='NO' after ALTER ENFORCED. "
                        + "Memgres reports 'YES'.");
            }
        }
    }
}
