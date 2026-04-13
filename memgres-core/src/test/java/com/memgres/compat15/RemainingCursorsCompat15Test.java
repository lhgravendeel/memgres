package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 2 PG vs Memgres cursor differences.
 *
 * These tests assert exact PG 18 behavior. They are expected to FAIL on
 * current Memgres, documenting the real gaps.
 *
 * Uses default JDBC (extended query protocol) to match the comparison framework.
 */
class RemainingCursorsCompat15Test {

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
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 90 (cursors.sql): WITH HOLD cursor should survive COMMIT.
     *
     * The comparison declares BOTH a regular cursor (c7_nohold) and a
     * WITH HOLD cursor (c7_hold) in the same transaction, commits, then
     * fetches from c7_nohold (error), then fetches from c7_hold.
     *
     * PG: FETCH c7_hold returns (id=1) after COMMIT.
     * Memgres: ERROR [34000] cursor "c7_hold" does not exist.
     */
    @Test
    void stmt90_withHoldCursorShouldSurviveCommit() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN");

            // Declare both cursors — matching the comparison scenario
            s.execute("DECLARE c7_nohold CURSOR FOR SELECT id FROM cur_compat_test ORDER BY id");
            s.execute("DECLARE c7_hold CURSOR WITH HOLD FOR SELECT id FROM cur_compat_test ORDER BY id");

            s.execute("COMMIT");

            // Regular cursor gone after COMMIT
            try {
                s.executeQuery("FETCH NEXT FROM c7_nohold");
            } catch (SQLException expected) {
                // Expected: cursor "c7_nohold" does not exist
            }

            // WITH HOLD cursor should still work after COMMIT
            try (ResultSet rs = s.executeQuery("FETCH NEXT FROM c7_hold")) {
                assertTrue(rs.next(), "WITH HOLD cursor should survive COMMIT and return a row");
                assertEquals(1, rs.getInt("id"),
                        "First FETCH from WITH HOLD cursor should return id=1");
            }

            try { s.execute("CLOSE c7_hold"); } catch (SQLException ignored) { }
        }
    }

    /**
     * Stmt 155 (pg-catalog-prepared-statements-cursors.sql): Default cursor
     * (declared without explicit SCROLL or NO SCROLL) should reject FETCH PRIOR.
     *
     * PG: ERROR [55000] cursor can only scan forward
     * Memgres: OK (?column?) 0 rows — incorrectly allows backward fetch
     */
    @Test
    void stmt155_defaultCursorShouldRejectFetchPrior() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN");
            // Plain cursor without SCROLL or NO SCROLL — PG defaults to forward-only
            s.execute("DECLARE cat_ns CURSOR FOR SELECT 1");

            try {
                s.executeQuery("FETCH PRIOR FROM cat_ns");
                fail("PG rejects FETCH PRIOR on a default (non-SCROLL) cursor with "
                        + "ERROR [55000], but Memgres succeeded");
            } catch (SQLException e) {
                assertEquals("55000", e.getSQLState(),
                        "SQLSTATE should be 55000 (cursor can only scan forward), got: "
                        + e.getSQLState() + " - " + e.getMessage());
            } finally {
                try { s.execute("ROLLBACK"); } catch (SQLException ignored) { }
            }
        }
    }
}
