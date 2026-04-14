package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compatibility tests for 13 cursor.sql failures where Memgres diverges from PG 18.
 *
 * Stmts 41-47: Default (no keyword) cursor in PG 18 allows backward fetch operations.
 *              Only explicitly declared NO SCROLL cursors reject them with SQLSTATE 55000.
 * Stmt 90:     WITH HOLD cursor should survive COMMIT; Memgres errors cursor does not exist
 * Stmt 129:    DECLARE inside txn with duplicate name should error 42P03, but outside txn
 *              should error 25P01; Memgres errors 42P03 (wrong SQLSTATE)
 * Stmts 185,188: FETCH FORWARD 0 / BACKWARD 0 should return current row; Memgres returns 0 rows
 * Stmt 226:    Cursor over UNION ALL query; Memgres errors cursor does not exist
 * Stmt 242:    DECLARE without WITH HOLD outside transaction should error 25P01; Memgres succeeds
 */
class CursorsCompat15Test {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS cur_test CASCADE");
            st.execute("CREATE TABLE cur_test ("
                    + "id   integer PRIMARY KEY,"
                    + "name text NOT NULL"
                    + ")");
            st.execute("INSERT INTO cur_test VALUES "
                    + "(1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta'), (5, 'epsilon')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try {
                conn.setAutoCommit(true);
                try (Statement st = conn.createStatement()) {
                    st.execute("DROP TABLE IF EXISTS cur_test CASCADE");
                }
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    @AfterEach
    void resetConnection() throws Exception {
        // Ensure we're back to a clean state after each test
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ignored) {
        }
    }

    // ========================================================================
    // Stmts 41-47: Default (no keyword) cursor in PG 18 allows backward fetch operations
    // ========================================================================

    /**
     * Stmt 41: FETCH PRIOR on a default (no keyword) cursor.
     *
     * PG 18: a cursor declared without SCROLL keyword allows backward fetch.
     * FETCH PRIOR from row 1 goes before first, returning 0 rows.
     */
    @Test
    void stmt41_fetchPriorOnNoScrollCursor() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id");
            st.executeQuery("FETCH NEXT FROM c4"); // move to row 1

            ResultSet rs = st.executeQuery("FETCH PRIOR FROM c4");
            assertFalse(rs.next(), "FETCH PRIOR from row 1 should return 0 rows (before first)");
            rs.close();

            st.execute("CLOSE c4");
        } finally {
            conn.rollback();
        }
    }

    /**
     * Stmt 42: FETCH LAST on a default (no keyword) cursor.
     *
     * PG 18: a cursor declared without SCROLL keyword allows backward fetch.
     * FETCH LAST returns the last row (id=5).
     */
    @Test
    void stmt42_fetchLastOnNoScrollCursor() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id");
            st.executeQuery("FETCH NEXT FROM c4"); // move to row 1

            ResultSet rs = st.executeQuery("FETCH LAST FROM c4");
            assertTrue(rs.next(), "FETCH LAST should return one row");
            assertEquals(5, rs.getInt("id"), "FETCH LAST should return id=5");
            assertFalse(rs.next());
            rs.close();

            st.execute("CLOSE c4");
        } finally {
            conn.rollback();
        }
    }

    /**
     * Stmt 43: FETCH FIRST on a default (no keyword) cursor.
     *
     * PG 18: a cursor declared without SCROLL keyword allows backward fetch.
     * FETCH FIRST returns the first row (id=1).
     */
    @Test
    void stmt43_fetchFirstOnNoScrollCursor() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id");
            st.executeQuery("FETCH NEXT FROM c4"); // move to row 1

            ResultSet rs = st.executeQuery("FETCH FIRST FROM c4");
            assertTrue(rs.next(), "FETCH FIRST should return one row");
            assertEquals(1, rs.getInt("id"), "FETCH FIRST should return id=1");
            assertFalse(rs.next());
            rs.close();

            st.execute("CLOSE c4");
        } finally {
            conn.rollback();
        }
    }

    /**
     * Stmt 44: FETCH ABSOLUTE 1 on a default (no keyword) cursor.
     *
     * PG 18: a cursor declared without SCROLL keyword allows backward fetch.
     * FETCH ABSOLUTE 1 returns the first row (id=1).
     */
    @Test
    void stmt44_fetchAbsoluteOnNoScrollCursor() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id");
            st.executeQuery("FETCH NEXT FROM c4"); // move to row 1

            ResultSet rs = st.executeQuery("FETCH ABSOLUTE 1 FROM c4");
            assertTrue(rs.next(), "FETCH ABSOLUTE 1 should return one row");
            assertEquals(1, rs.getInt("id"), "FETCH ABSOLUTE 1 should return id=1");
            assertFalse(rs.next());
            rs.close();

            st.execute("CLOSE c4");
        } finally {
            conn.rollback();
        }
    }

    /**
     * Stmt 45: FETCH RELATIVE -1 on a default (no keyword) cursor.
     *
     * PG 18: a cursor declared without SCROLL keyword allows backward fetch.
     * FETCH RELATIVE -1 from row 1 goes before first, returning 0 rows.
     */
    @Test
    void stmt45_fetchRelativeNegativeOnNoScrollCursor() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id");
            st.executeQuery("FETCH NEXT FROM c4"); // move to row 1

            ResultSet rs = st.executeQuery("FETCH RELATIVE -1 FROM c4");
            assertFalse(rs.next(), "FETCH RELATIVE -1 from row 1 should return 0 rows (before first)");
            rs.close();

            st.execute("CLOSE c4");
        } finally {
            conn.rollback();
        }
    }

    /**
     * Stmt 46: FETCH RELATIVE 1 on a NO SCROLL cursor.
     *
     * PG 18 allows positive RELATIVE on a default cursor; returns row [1].
     * (After RELATIVE -1 from row 1, cursor is before-first; RELATIVE 1 returns first row.)
     * In our isolated test, after NEXT to row 1, RELATIVE 1 returns row [2].
     * Memgres errors: "cursor \"c4\" does not exist" [34000]
     */
    @Test
    void stmt46_fetchRelativePositiveOnNoScrollCursor() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id");
            st.executeQuery("FETCH NEXT FROM c4"); // at row 1

            // RELATIVE 1 from row 1 => row 2
            ResultSet rs = st.executeQuery("FETCH RELATIVE 1 FROM c4");
            assertTrue(rs.next(), "FETCH RELATIVE 1 should return one row");
            int id = rs.getInt("id");
            // In PG's original sequence (after ABSOLUTE 1 then RELATIVE -1 back to before-first),
            // RELATIVE 1 returns row 1. In our isolated test from row 1, RELATIVE 1 returns row 2.
            // We test the isolated behavior here.
            assertEquals(2, id, "FETCH RELATIVE 1 from row 1 should return id=2");
            assertFalse(rs.next());
            rs.close();

            st.execute("CLOSE c4");
        } finally {
            conn.rollback();
        }
    }

    /**
     * Stmt 47: FETCH BACKWARD 1 on a default (no keyword) cursor.
     *
     * PG 18: a cursor declared without SCROLL keyword allows backward fetch.
     * FETCH BACKWARD 1 from row 1 goes before first, returning 0 rows.
     */
    @Test
    void stmt47_fetchBackwardOnNoScrollCursor() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id");
            st.executeQuery("FETCH NEXT FROM c4"); // at row 1

            ResultSet rs = st.executeQuery("FETCH BACKWARD 1 FROM c4");
            assertFalse(rs.next(), "FETCH BACKWARD 1 from row 1 should return 0 rows (before first)");
            rs.close();

            st.execute("CLOSE c4");
        } finally {
            conn.rollback();
        }
    }

    // ========================================================================
    // Stmt 90: WITH HOLD cursor should survive COMMIT
    // ========================================================================

    /**
     * Stmt 90: FETCH NEXT from a WITH HOLD cursor after COMMIT.
     *
     * A cursor declared WITH HOLD should survive a COMMIT and remain usable.
     * PG 18: returns row [1] (first row of cur_test)
     * Memgres: errors "cursor \"c7_hold\" does not exist" [34000]
     */
    @Test
    void stmt90_withHoldCursorSurvivesCommit() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c7_hold CURSOR WITH HOLD FOR SELECT id FROM cur_test ORDER BY id");
            conn.commit();

            // After COMMIT, holdable cursor should still be accessible
            ResultSet rs = st.executeQuery("FETCH NEXT FROM c7_hold");
            assertTrue(rs.next(), "WITH HOLD cursor should survive COMMIT and return a row");
            assertEquals(1, rs.getInt("id"), "First row should have id=1");
            assertFalse(rs.next());
            rs.close();

            st.execute("CLOSE c7_hold");
            conn.commit();
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        }
    }

    // ========================================================================
    // Stmt 129: DECLARE with duplicate name outside txn should error 25P01
    // ========================================================================

    /**
     * Stmt 129: DECLARE CURSOR (non-holdable) outside a transaction block should
     * produce SQLSTATE 25P01 ("no active SQL transaction" / "can only be used in
     * transaction blocks").
     *
     * PG 18: ERROR [25P01]: DECLARE CURSOR can only be used in transaction blocks
     * Memgres: ERROR [42P03]: cursor "c12" already exists (wrong SQLSTATE)
     */
    @Test
    void stmt129_declareCursorOutsideTransactionErrors25P01() throws SQLException {
        conn.setAutoCommit(true);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c12 CURSOR FOR SELECT 2");
            fail("DECLARE CURSOR (non-holdable) outside transaction should fail with SQLSTATE 25P01");
        } catch (SQLException e) {
            assertEquals("25P01", e.getSQLState(),
                    "Expected SQLSTATE 25P01 (no active SQL transaction), got: "
                            + e.getSQLState() + " - " + e.getMessage());
            assertTrue(e.getMessage().toLowerCase().contains("transaction"),
                    "Error message should mention 'transaction', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmts 185, 188: FETCH FORWARD 0 / BACKWARD 0 should return current row
    // ========================================================================

    /**
     * Stmt 185: FETCH FORWARD 0 should return the current row without moving.
     *
     * With a SCROLL cursor positioned at row 3 (via FETCH ABSOLUTE 3),
     * FETCH FORWARD 0 should return that row.
     * PG 18: returns row [3]
     * Memgres: returns 0 rows
     */
    @Test
    void stmt185_fetchForward0ReturnsCurrentRow() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c21 SCROLL CURSOR FOR SELECT id FROM cur_test ORDER BY id");

            // Position at row 3
            ResultSet rs = st.executeQuery("FETCH ABSOLUTE 3 FROM c21");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            rs.close();

            // FETCH FORWARD 0 should return current row (3) without moving
            rs = st.executeQuery("FETCH FORWARD 0 FROM c21");
            assertTrue(rs.next(), "FETCH FORWARD 0 should return the current row");
            assertEquals(3, rs.getInt("id"), "FETCH FORWARD 0 should return id=3 (current position)");
            assertFalse(rs.next(), "FETCH FORWARD 0 should return exactly one row");
            rs.close();

            // Verify position unchanged: NEXT should return row 4
            rs = st.executeQuery("FETCH NEXT FROM c21");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("id"), "After FORWARD 0, NEXT should return row 4 (position unchanged)");
            rs.close();

            st.execute("CLOSE c21");
        } finally {
            conn.rollback();
        }
    }

    /**
     * Stmt 188: FETCH BACKWARD 0 should return the current row without moving.
     *
     * With a SCROLL cursor positioned at row 2 (via FETCH ABSOLUTE 2),
     * FETCH BACKWARD 0 should return that row.
     * PG 18: returns row [2]
     * Memgres: returns 0 rows
     */
    @Test
    void stmt188_fetchBackward0ReturnsCurrentRow() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c21 SCROLL CURSOR FOR SELECT id FROM cur_test ORDER BY id");

            // Position at row 2
            ResultSet rs = st.executeQuery("FETCH ABSOLUTE 2 FROM c21");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            rs.close();

            // FETCH BACKWARD 0 should return current row (2) without moving
            rs = st.executeQuery("FETCH BACKWARD 0 FROM c21");
            assertTrue(rs.next(), "FETCH BACKWARD 0 should return the current row");
            assertEquals(2, rs.getInt("id"), "FETCH BACKWARD 0 should return id=2 (current position)");
            assertFalse(rs.next(), "FETCH BACKWARD 0 should return exactly one row");
            rs.close();

            // Verify position unchanged: NEXT should return row 3
            rs = st.executeQuery("FETCH NEXT FROM c21");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"), "After BACKWARD 0, NEXT should return row 3 (position unchanged)");
            rs.close();

            st.execute("CLOSE c21");
        } finally {
            conn.rollback();
        }
    }

    // ========================================================================
    // Stmt 226: Cursor with UNION ALL query
    // ========================================================================

    /**
     * Stmt 226: FETCH ALL from a cursor over a UNION ALL query.
     *
     * The cursor selects rows with id <= 2 UNION ALL rows with id = 5, ordered by id.
     * PG 18: returns [1|alpha], [2|beta], [5|epsilon]
     * Memgres: errors "cursor \"c27\" does not exist" [34000]
     */
    @Test
    void stmt226_cursorWithUnionAll() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c27 CURSOR FOR "
                    + "SELECT id, name FROM cur_test WHERE id <= 2 "
                    + "UNION ALL "
                    + "SELECT id, name FROM cur_test WHERE id = 5 "
                    + "ORDER BY id");

            ResultSet rs = st.executeQuery("FETCH ALL FROM c27");

            List<Integer> ids = new ArrayList<>();
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                names.add(rs.getString("name"));
            }
            rs.close();

            assertEquals(List.of(1, 2, 5), ids,
                    "FETCH ALL from UNION ALL cursor should return ids [1, 2, 5]");
            assertEquals(List.of("alpha", "beta", "epsilon"), names,
                    "FETCH ALL from UNION ALL cursor should return names [alpha, beta, epsilon]");

            st.execute("CLOSE c27");
        } finally {
            conn.rollback();
        }
    }

    // ========================================================================
    // Stmt 242: DECLARE without WITH HOLD outside transaction should error
    // ========================================================================

    /**
     * Stmt 242: DECLARE CURSOR (non-holdable) outside a transaction block should
     * produce SQLSTATE 25P01.
     *
     * PG 18: ERROR [25P01]: DECLARE CURSOR can only be used in transaction blocks
     * Memgres: succeeds (OK, 0 rows affected) — should have failed
     */
    @Test
    void stmt242_declareNoHoldOutsideTransactionShouldError() throws SQLException {
        conn.setAutoCommit(true);
        try (Statement st = conn.createStatement()) {
            st.execute("DECLARE c30_nohold CURSOR FOR SELECT 1");
            fail("DECLARE non-holdable cursor outside transaction should fail with SQLSTATE 25P01");
        } catch (SQLException e) {
            assertEquals("25P01", e.getSQLState(),
                    "Expected SQLSTATE 25P01 (DECLARE CURSOR can only be used in transaction blocks), got: "
                            + e.getSQLState() + " - " + e.getMessage());
        }
    }
}
