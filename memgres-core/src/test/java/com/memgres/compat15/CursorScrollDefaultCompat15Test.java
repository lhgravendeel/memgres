package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PG 18 cursor scrollability behavior.
 *
 * In PG 18, cursors declared without SCROLL or NO SCROLL are effectively
 * scrollable — backward fetch directions return 0 rows instead of erroring.
 * Only cursors explicitly declared NO SCROLL reject backward movement.
 *
 * These tests cover the 9 remaining differences from the feature-comparison report:
 * - cursors.sql stmts 41-47: backward fetch on default (no keyword) cursor
 * - cursors.sql stmt 90: WITH HOLD cursor survives COMMIT
 * - pg-catalog stmt 74: pg_cursors.is_scrollable for default cursor
 */
class CursorScrollDefaultCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(false);

        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS cur_scroll_test (id integer PRIMARY KEY)");
            s.execute("DELETE FROM cur_scroll_test");
            for (int i = 1; i <= 5; i++) {
                s.execute("INSERT INTO cur_scroll_test VALUES (" + i + ")");
            }
            conn.commit();
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS cur_scroll_test");
                conn.commit();
            } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    @AfterEach
    void rollbackAfterEach() throws Exception {
        try { conn.rollback(); } catch (SQLException ignored) {}
    }

    // ========================================================================
    // Stmt 41: FETCH PRIOR on default cursor — PG returns 0 rows (no error)
    // ========================================================================
    @Test
    void stmt41_fetchPriorOnDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            // Move forward one position
            ResultSet rs1 = s.executeQuery("FETCH NEXT FROM c4");
            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt("id"));
            rs1.close();

            // FETCH PRIOR — PG returns 0 rows, does NOT error
            ResultSet rs = s.executeQuery("FETCH PRIOR FROM c4");
            assertFalse(rs.next(), "FETCH PRIOR on default cursor should return 0 rows, not error");
            rs.close();
        }
    }

    // ========================================================================
    // Stmt 42: FETCH LAST on default cursor — PG returns row [5]
    // ========================================================================
    @Test
    void stmt42_fetchLastOnDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c4").close(); // position at row 1

            ResultSet rs = s.executeQuery("FETCH LAST FROM c4");
            assertTrue(rs.next(), "FETCH LAST on default cursor should return a row");
            assertEquals(5, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        }
    }

    // ========================================================================
    // Stmt 43: FETCH FIRST on default cursor — PG returns row [1]
    // ========================================================================
    @Test
    void stmt43_fetchFirstOnDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c4").close();

            ResultSet rs = s.executeQuery("FETCH FIRST FROM c4");
            assertTrue(rs.next(), "FETCH FIRST on default cursor should return a row");
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        }
    }

    // ========================================================================
    // Stmt 44: FETCH ABSOLUTE 1 on default cursor — PG returns row [1]
    // ========================================================================
    @Test
    void stmt44_fetchAbsoluteOnDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c4").close();

            ResultSet rs = s.executeQuery("FETCH ABSOLUTE 1 FROM c4");
            assertTrue(rs.next(), "FETCH ABSOLUTE 1 on default cursor should return a row");
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        }
    }

    // ========================================================================
    // Stmt 45: FETCH RELATIVE -1 on default cursor — PG returns 0 rows
    // (cursor at row 1 after FIRST/ABSOLUTE, RELATIVE -1 goes to before-first)
    // ========================================================================
    @Test
    void stmt45_fetchRelativeNegativeOnDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c4").close(); // position at row 1

            ResultSet rs = s.executeQuery("FETCH RELATIVE -1 FROM c4");
            assertFalse(rs.next(), "FETCH RELATIVE -1 from row 1 should return 0 rows");
            rs.close();
        }
    }

    // ========================================================================
    // Stmt 46: FETCH RELATIVE 1 on default cursor — PG returns row [1]
    // (after ABSOLUTE 1, position=0, RELATIVE 1 goes to row index 1 = id 2)
    // Actually in the comparison file, after FIRST, cursor is at row 0 (id=1),
    // RELATIVE 1 goes to row 1 (id=2). But annotation says row [1].
    // The comparison SQL sequence: NEXT(→1), PRIOR(→0rows), LAST(→5),
    // FIRST(→1), ABSOLUTE 1(→1), RELATIVE -1(→0rows), RELATIVE 1(→1)
    // After RELATIVE -1 from ABSOLUTE 1, cursor is before-first. RELATIVE 1 → row 0 = id 1.
    // ========================================================================
    @Test
    void stmt46_fetchRelativePositiveOnDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c4").close();

            // This is just forward relative — should work even on no-scroll
            ResultSet rs = s.executeQuery("FETCH RELATIVE 1 FROM c4");
            assertTrue(rs.next(), "FETCH RELATIVE 1 on default cursor should return a row");
            rs.close();
        }
    }

    // ========================================================================
    // Stmt 47: FETCH BACKWARD 1 on default cursor — PG returns 0 rows
    // ========================================================================
    @Test
    void stmt47_fetchBackwardOnDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c4 CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c4").close();

            ResultSet rs = s.executeQuery("FETCH BACKWARD 1 FROM c4");
            assertFalse(rs.next(), "FETCH BACKWARD on default cursor should return 0 rows, not error");
            rs.close();
        }
    }

    // ========================================================================
    // Stmt 90: WITH HOLD cursor survives COMMIT — FETCH NEXT returns row [1]
    // ========================================================================
    @Test
    void stmt90_withHoldCursorSurvivesCommit() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c7_hold CURSOR WITH HOLD FOR SELECT id FROM cur_scroll_test ORDER BY id");
            conn.commit(); // cursor should survive

            ResultSet rs = s.executeQuery("FETCH NEXT FROM c7_hold");
            assertTrue(rs.next(), "WITH HOLD cursor should survive COMMIT");
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();

            s.execute("CLOSE c7_hold");
        }
    }

    // ========================================================================
    // Stmt 74: pg_cursors.is_scrollable = true for default cursor
    // ========================================================================
    @Test
    void stmt74_pgCursorsIsScrollableForDefaultCursor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_cur CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");

            ResultSet rs = s.executeQuery(
                    "SELECT name, is_holdable::text, is_binary::text, is_scrollable::text "
                    + "FROM pg_cursors WHERE name = 'cat_cur'");
            assertTrue(rs.next(), "Expected one row in pg_cursors for cat_cur");
            assertEquals("cat_cur", rs.getString("name"));
            assertEquals("false", rs.getString("is_holdable"));
            assertEquals("false", rs.getString("is_binary"));
            assertEquals("true", rs.getString("is_scrollable"),
                    "PG reports is_scrollable=true for default (no keyword) cursors");
            assertFalse(rs.next());
            rs.close();
        }
    }

    // ========================================================================
    // Explicit NO SCROLL should still reject backward movement
    // ========================================================================
    @Test
    void explicitNoScrollRejectsBackward() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c_noscroll NO SCROLL CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c_noscroll").close();

            try {
                s.executeQuery("FETCH PRIOR FROM c_noscroll");
                fail("Explicit NO SCROLL cursor should reject FETCH PRIOR");
            } catch (SQLException e) {
                assertEquals("55000", e.getSQLState());
            }
        }
    }

    // ========================================================================
    // Explicit SCROLL should allow backward movement (regression check)
    // ========================================================================
    @Test
    void explicitScrollAllowsBackward() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE c_scroll SCROLL CURSOR FOR SELECT id FROM cur_scroll_test ORDER BY id");
            s.executeQuery("FETCH NEXT FROM c_scroll").close();

            ResultSet rs = s.executeQuery("FETCH PRIOR FROM c_scroll");
            assertFalse(rs.next(), "FETCH PRIOR from row 1 returns 0 rows (before-first)");
            rs.close();
        }
    }
}
