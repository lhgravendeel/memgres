package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Row-level locking variants.
 *
 * - FOR NO KEY UPDATE
 * - FOR KEY SHARE
 * - FOR UPDATE OF <table_list>
 * - Lock-mode upgrade (share → exclusive)
 * - SKIP LOCKED + ORDER BY + LIMIT
 */
class Round14RowLockingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    @BeforeEach
    void seed() throws SQLException {
        exec("DROP TABLE IF EXISTS r14_lk CASCADE");
        exec("CREATE TABLE r14_lk (id int PRIMARY KEY, v int)");
        exec("INSERT INTO r14_lk VALUES (1,10),(2,20),(3,30),(4,40),(5,50)");
    }

    // =========================================================================
    // A. FOR NO KEY UPDATE / FOR KEY SHARE / FOR SHARE
    // =========================================================================

    @Test
    void select_for_no_key_update() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM r14_lk WHERE id = 1 FOR NO KEY UPDATE")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void select_for_key_share() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM r14_lk WHERE id = 1 FOR KEY SHARE")) {
            assertTrue(rs.next());
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void select_for_share() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM r14_lk WHERE id = 1 FOR SHARE")) {
            assertTrue(rs.next());
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // B. FOR UPDATE OF <table_list>
    // =========================================================================

    @Test
    void select_for_update_of_specific_table() throws SQLException {
        exec("CREATE TABLE r14_lk_b (id int)");
        exec("INSERT INTO r14_lk_b VALUES (1)");
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a.id FROM r14_lk a JOIN r14_lk_b b ON a.id = b.id "
                             + "WHERE a.id = 1 FOR UPDATE OF a")) {
            assertTrue(rs.next());
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // C. SKIP LOCKED with ORDER BY + LIMIT (worker-queue pattern)
    // =========================================================================

    @Test
    void for_update_skip_locked_with_order_by_limit() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id FROM r14_lk ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED")) {
            int n = 0;
            while (rs.next()) n++;
            assertEquals(2, n);
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void for_update_nowait_on_blocked_row_errors() throws Exception {
        // Thread 1 locks row; thread 2 does FOR UPDATE NOWAIT → 55P03.
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM r14_lk WHERE id = 1 FOR UPDATE")) {
            assertTrue(rs.next());

            // Second connection attempts NOWAIT → should error with SQLSTATE 55P03
            try (Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {
                c2.setAutoCommit(false);
                SQLException ex = assertThrows(SQLException.class, () -> {
                    try (Statement s2 = c2.createStatement();
                         ResultSet rs2 = s2.executeQuery(
                                 "SELECT id FROM r14_lk WHERE id = 1 FOR UPDATE NOWAIT")) {
                        rs2.next();
                    }
                });
                String state = ex.getSQLState();
                assertTrue("55P03".equals(state) || ex.getMessage().toLowerCase().contains("could not"),
                        "NOWAIT must signal lock-not-available; got state=" + state);
                c2.rollback();
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // D. Lock mode upgrade: SHARE → EXCLUSIVE in same txn
    // =========================================================================

    @Test
    void lock_mode_upgrade_within_txn() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            // Take share lock first, then request exclusive in same txn — should succeed.
            try (ResultSet rs = s.executeQuery("SELECT id FROM r14_lk WHERE id = 1 FOR SHARE")) {
                assertTrue(rs.next());
            }
            try (ResultSet rs = s.executeQuery("SELECT id FROM r14_lk WHERE id = 1 FOR UPDATE")) {
                assertTrue(rs.next());
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // E. FOR UPDATE within subquery
    // =========================================================================

    @Test
    void for_update_in_cte_with_returning_pattern() throws Exception {
        // Classic worker-queue pattern: DELETE ... USING (SELECT ... FOR UPDATE SKIP LOCKED)
        conn.setAutoCommit(false);
        try {
            exec("DELETE FROM r14_lk WHERE id IN ("
                    + "  SELECT id FROM r14_lk WHERE v >= 20 "
                    + "  ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED"
                    + ")");
            conn.commit();
            // 2 rows should have been deleted
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*)::int FROM r14_lk")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // F. Row-locking with WAIT (explicit)
    // =========================================================================

    @Test
    void for_update_wait_is_default() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id FROM r14_lk WHERE id = 1 FOR UPDATE")) {
            assertTrue(rs.next());
            // default: no NOWAIT/SKIP LOCKED modifier
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }
}
