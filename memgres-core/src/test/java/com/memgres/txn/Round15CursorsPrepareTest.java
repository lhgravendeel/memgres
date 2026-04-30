package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category B: SQL-level cursors + prepared statements.
 *
 * Covers:
 *  - DECLARE INSENSITIVE / BINARY / SCROLL / WITH HOLD
 *  - FETCH BACKWARD / RELATIVE / ABSOLUTE positioning
 *  - MOVE
 *  - WHERE CURRENT OF
 *  - PREPARE / EXECUTE / DEALLOCATE + pg_prepared_statements
 *  - pg_cursors view
 */
class Round15CursorsPrepareTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r15_cur");
            s.execute("CREATE TABLE r15_cur (id int PRIMARY KEY, label text)");
            s.execute("INSERT INTO r15_cur VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')");
            conn.commit();
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. DECLARE INSENSITIVE / BINARY / SCROLL / WITH HOLD
    // =========================================================================

    @Test
    void declare_insensitive_cursor() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_ins INSENSITIVE CURSOR FOR SELECT id FROM r15_cur ORDER BY id");
        // Modify underlying table — insensitive cursor shouldn't see change
        exec("UPDATE r15_cur SET label='X' WHERE id=1");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH 1 FROM c_ins")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        exec("CLOSE c_ins");
        exec("ROLLBACK");
    }

    @Test
    void declare_binary_cursor() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_bin BINARY CURSOR FOR SELECT id FROM r15_cur ORDER BY id");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH 1 FROM c_bin")) {
            assertTrue(rs.next());
        }
        exec("CLOSE c_bin");
        exec("ROLLBACK");
    }

    @Test
    void declare_scroll_with_hold_outside_txn_block() throws SQLException {
        // WITH HOLD cursors can outlive transactions
        exec("BEGIN");
        exec("DECLARE c_hold SCROLL CURSOR WITH HOLD FOR SELECT id FROM r15_cur ORDER BY id");
        exec("COMMIT");
        // Now cursor should be usable outside the txn
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH 1 FROM c_hold")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        exec("CLOSE c_hold");
    }

    // =========================================================================
    // B. FETCH positioning (BACKWARD, RELATIVE, ABSOLUTE, PRIOR, MOVE)
    // =========================================================================

    @Test
    void fetch_backward_returns_correct_rows() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_b SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id");
        // Move to row 3
        exec("MOVE 3 FROM c_b");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH BACKWARD 1 FROM c_b")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1),
                    "BACKWARD 1 after MOVE 3 should return row 2, not something else");
        }
        // Another BACKWARD 1 — should now return row 1
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH BACKWARD 1 FROM c_b")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1),
                    "Consecutive BACKWARD calls should walk back through rows");
        }
        exec("CLOSE c_b");
        exec("ROLLBACK");
    }

    @Test
    void fetch_absolute_positions_cursor() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_abs SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH ABSOLUTE 3 FROM c_abs")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH ABSOLUTE 1 FROM c_abs")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        exec("CLOSE c_abs");
        exec("ROLLBACK");
    }

    @Test
    void fetch_relative_positions_cursor() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_rel SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id");
        exec("MOVE 2 FROM c_rel");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH RELATIVE 2 FROM c_rel")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1), "RELATIVE 2 from row 2 should give row 4");
        }
        exec("CLOSE c_rel");
        exec("ROLLBACK");
    }

    @Test
    void fetch_prior_returns_previous_row() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_p SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id");
        exec("MOVE 3 FROM c_p");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH PRIOR FROM c_p")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
        exec("CLOSE c_p");
        exec("ROLLBACK");
    }

    @Test
    void fetch_forward_zero_refetches_current_row() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_z SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id");
        exec("MOVE 2 FROM c_z");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH FORWARD 0 FROM c_z")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1),
                    "FORWARD 0 should return the current row, not advance");
        }
        exec("CLOSE c_z");
        exec("ROLLBACK");
    }

    // =========================================================================
    // C. WHERE CURRENT OF
    // =========================================================================

    @Test
    void update_where_current_of() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_upd CURSOR FOR SELECT id, label FROM r15_cur WHERE id=2 FOR UPDATE");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH 1 FROM c_upd")) {
            assertTrue(rs.next());
        }
        exec("UPDATE r15_cur SET label='UPDATED' WHERE CURRENT OF c_upd");
        int n = scalarInt("SELECT count(*)::int FROM r15_cur WHERE label='UPDATED'");
        assertEquals(1, n, "WHERE CURRENT OF should update exactly the positioned row");
        exec("CLOSE c_upd");
        exec("ROLLBACK");
    }

    @Test
    void delete_where_current_of() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_del CURSOR FOR SELECT id FROM r15_cur WHERE id=3 FOR UPDATE");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("FETCH 1 FROM c_del")) {
            assertTrue(rs.next());
        }
        exec("DELETE FROM r15_cur WHERE CURRENT OF c_del");
        int n = scalarInt("SELECT count(*)::int FROM r15_cur WHERE id=3");
        assertEquals(0, n, "WHERE CURRENT OF should delete the positioned row");
        exec("CLOSE c_del");
        exec("ROLLBACK");
    }

    // =========================================================================
    // D. pg_cursors view
    // =========================================================================

    @Test
    void pg_cursors_view_populated() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_view CURSOR FOR SELECT 1");
        int n = scalarInt("SELECT count(*)::int FROM pg_cursors WHERE name = 'c_view'");
        assertEquals(1, n, "pg_cursors must show the DECLAREd cursor");
        exec("CLOSE c_view");
        exec("ROLLBACK");
    }

    @Test
    void pg_cursors_has_standard_columns() throws SQLException {
        // Should have: name, statement, is_holdable, is_binary, is_scrollable, creation_time
        exec("BEGIN");
        exec("DECLARE c_cols CURSOR FOR SELECT 1");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT name, statement, is_holdable, is_binary, is_scrollable, creation_time"
                             + " FROM pg_cursors WHERE name='c_cols'")) {
            assertTrue(rs.next());
            assertEquals("c_cols", rs.getString("name"));
            assertNotNull(rs.getString("statement"));
            rs.getBoolean("is_holdable");
            rs.getBoolean("is_binary");
            rs.getBoolean("is_scrollable");
            assertNotNull(rs.getTimestamp("creation_time"));
        }
        exec("CLOSE c_cols");
        exec("ROLLBACK");
    }

    @Test
    void close_all_cursors() throws SQLException {
        exec("BEGIN");
        exec("DECLARE c_a CURSOR FOR SELECT 1");
        exec("DECLARE c_b CURSOR FOR SELECT 2");
        exec("CLOSE ALL");
        int n = scalarInt("SELECT count(*)::int FROM pg_cursors");
        // PG keeps the implicit unnamed portal cursor even after CLOSE ALL, so count=1
        assertTrue(n <= 1, "CLOSE ALL must drop all declared cursors (implicit portal may remain)");
        exec("ROLLBACK");
    }

    // =========================================================================
    // E. PREPARE / EXECUTE / DEALLOCATE
    // =========================================================================

    @Test
    void prepare_execute_deallocate() throws SQLException {
        exec("BEGIN");
        exec("PREPARE r15_p1 (int) AS SELECT id, label FROM r15_cur WHERE id = $1");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE r15_p1 (2)")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
        int n = scalarInt("SELECT count(*)::int FROM pg_prepared_statements WHERE name='r15_p1'");
        assertEquals(1, n, "PREPARE must register in pg_prepared_statements");
        exec("DEALLOCATE r15_p1");
        n = scalarInt("SELECT count(*)::int FROM pg_prepared_statements WHERE name='r15_p1'");
        assertEquals(0, n, "DEALLOCATE must remove prepared statement");
        exec("ROLLBACK");
    }

    @Test
    void deallocate_all_drops_sql_prepared_statements() throws SQLException {
        exec("BEGIN");
        exec("PREPARE r15_p2 AS SELECT 1");
        exec("PREPARE r15_p3 AS SELECT 2");
        exec("DEALLOCATE ALL");
        // PG's DEALLOCATE ALL drops only the user-level ones
        int n = scalarInt("SELECT count(*)::int FROM pg_prepared_statements "
                + "WHERE name IN ('r15_p2','r15_p3')");
        assertEquals(0, n);
        exec("ROLLBACK");
    }

    @Test
    void pg_prepared_statements_columns() throws SQLException {
        exec("BEGIN");
        exec("PREPARE r15_p4 (int, text) AS SELECT $1, $2");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT name, statement, prepare_time, parameter_types, from_sql, generic_plans, custom_plans"
                             + " FROM pg_prepared_statements WHERE name='r15_p4'")) {
            assertTrue(rs.next());
            assertEquals("r15_p4", rs.getString("name"));
            assertNotNull(rs.getString("statement"));
            assertNotNull(rs.getTimestamp("prepare_time"));
            Object pt = rs.getObject("parameter_types");
            assertNotNull(pt, "parameter_types array must be populated");
            assertTrue(rs.getBoolean("from_sql"),
                    "from_sql must be true for SQL-level PREPARE");
            // generic_plans / custom_plans are counters (PG 17+)
            rs.getLong("generic_plans");
            rs.getLong("custom_plans");
        }
        exec("DEALLOCATE r15_p4");
        exec("ROLLBACK");
    }

    @Test
    void execute_cant_reference_nonexistent_prepared_stmt() throws SQLException {
        exec("BEGIN");
        try {
            exec("EXECUTE r15_doesnotexist");
            fail("EXECUTE of nonexistent statement should raise error");
        } catch (SQLException e) {
            // PG: 26000 (invalid_sql_statement_name)
            assertNotNull(e.getMessage());
        }
        exec("ROLLBACK");
    }
}
