package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 2 remaining Memgres-vs-PG cursor differences.
 *
 * These tests assert PG 18 behavior. They are expected to FAIL on current
 * Memgres and pass once the underlying issues are fixed.
 */
class RemainingCursorsCompat15Test {

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
            s.execute("DROP TABLE IF EXISTS cur_compat_test CASCADE");
            s.execute("CREATE TABLE cur_compat_test (id integer PRIMARY KEY, name text NOT NULL)");
            s.execute("INSERT INTO cur_compat_test VALUES (1, 'alpha'), (2, 'beta'), "
                    + "(3, 'gamma'), (4, 'delta'), (5, 'epsilon')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS cur_compat_test CASCADE");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 90 (cursors.sql): WITH HOLD cursor should survive COMMIT
    // PG: FETCH NEXT FROM c7_hold returns [1] after COMMIT
    // Memgres: ERROR [34000] cursor "c7_hold" does not exist
    // ========================================================================
    @Test
    void stmt90_withHoldCursorShouldSurviveCommit() throws Exception {
        try (Statement s = conn.createStatement()) {
            // Start a transaction, declare a WITH HOLD cursor, then COMMIT
            s.execute("BEGIN");
            s.execute("DECLARE c7_hold CURSOR WITH HOLD FOR SELECT id FROM cur_compat_test ORDER BY id");
            s.execute("COMMIT");

            // WITH HOLD cursor should still be accessible after COMMIT
            try (ResultSet rs = s.executeQuery("FETCH NEXT FROM c7_hold")) {
                assertTrue(rs.next(), "Expected one row from WITH HOLD cursor after COMMIT");
                assertEquals(1, rs.getInt("id"),
                        "First FETCH from WITH HOLD cursor should return id=1");
            }

            // Clean up cursor
            s.execute("CLOSE c7_hold");
        }
    }

    // ========================================================================
    // Stmt 155: Cursor declared with explicit NO SCROLL should reject FETCH PRIOR.
    // PG behavior varies for implicit scrollability, but NO SCROLL is always strict.
    // ========================================================================
    @Test
    void stmt155_noScrollCursorShouldRejectFetchPrior() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN");
            s.execute("DECLARE cat_ns NO SCROLL CURSOR FOR SELECT 1");

            try {
                s.executeQuery("FETCH PRIOR FROM cat_ns");
                fail("Expected error for FETCH PRIOR on NO SCROLL cursor, but query succeeded");
            } catch (SQLException e) {
                assertEquals("55000", e.getSQLState(),
                        "SQLSTATE should be 55000 (cursor can only scan forward), got: "
                        + e.getSQLState() + " - " + e.getMessage());
            } finally {
                try {
                    s.execute("ROLLBACK");
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
