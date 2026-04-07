package com.memgres.txn;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 1: Locking and concurrency.
 * Tests FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR KEY SHARE,
 * NOWAIT, SKIP LOCKED, lock timeouts, deadlock detection.
 */
class LockingConcurrencyTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    // --- FOR UPDATE ---

    @Test void select_for_update_basic() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_t1(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO lock_t1 VALUES (1,'a'),(2,'b'),(3,'c')");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM lock_t1 WHERE id = 1 FOR UPDATE")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("a", rs.getString(2));
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_t1");
            c.commit();
        }
    }

    @Test void select_for_update_with_order_by() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_t2(id int PRIMARY KEY, v int)");
            c.createStatement().execute("INSERT INTO lock_t2 VALUES (1,30),(2,10),(3,20)");
            c.commit();
            List<Integer> ids = new ArrayList<>();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_t2 ORDER BY v FOR UPDATE")) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            assertEquals(Cols.listOf(2, 3, 1), ids, "ORDER BY should work with FOR UPDATE");
            c.commit();
            c.createStatement().execute("DROP TABLE lock_t2");
            c.commit();
        }
    }

    @Test void select_for_update_with_where_no_rows() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_t3(id int PRIMARY KEY)");
            c.createStatement().execute("INSERT INTO lock_t3 VALUES (1),(2),(3)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_t3 WHERE id = 999 FOR UPDATE")) {
                assertFalse(rs.next(), "No rows should be returned or locked");
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_t3");
            c.commit();
        }
    }

    @Test void select_for_update_with_limit() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_t4(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO lock_t4 VALUES (1,'a'),(2,'b'),(3,'c')");
            c.commit();
            List<Integer> ids = new ArrayList<>();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_t4 ORDER BY id FOR UPDATE LIMIT 2")) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            assertEquals(2, ids.size(), "Only 2 rows should be locked and returned");
            c.commit();
            c.createStatement().execute("DROP TABLE lock_t4");
            c.commit();
        }
    }

    // --- FOR NO KEY UPDATE ---

    @Test void select_for_no_key_update() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_nku(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO lock_nku VALUES (1,'x')");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_nku WHERE id = 1 FOR NO KEY UPDATE")) {
                assertTrue(rs.next());
                assertEquals("x", rs.getString(2));
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_nku");
            c.commit();
        }
    }

    // --- FOR SHARE ---

    @Test void select_for_share_basic() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_sh(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO lock_sh VALUES (1,'shared')");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_sh FOR SHARE")) {
                assertTrue(rs.next());
                assertEquals("shared", rs.getString(2));
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_sh");
            c.commit();
        }
    }

    @Test void select_for_share_multiple_sessions_read() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute("CREATE TABLE lock_sh2(id int PRIMARY KEY, v text)");
            c1.createStatement().execute("INSERT INTO lock_sh2 VALUES (1,'s')");
            c1.commit();
            // Both sessions should be able to acquire FOR SHARE simultaneously
            try (ResultSet rs1 = c1.createStatement().executeQuery(
                    "SELECT * FROM lock_sh2 FOR SHARE")) {
                assertTrue(rs1.next());
                try (ResultSet rs2 = c2.createStatement().executeQuery(
                        "SELECT * FROM lock_sh2 FOR SHARE")) {
                    assertTrue(rs2.next(), "Second session should also get FOR SHARE lock");
                }
            }
            c1.commit();
            c2.commit();
            c1.createStatement().execute("DROP TABLE lock_sh2");
            c1.commit();
        }
    }

    // --- FOR KEY SHARE ---

    @Test void select_for_key_share() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_ks(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO lock_ks VALUES (1,'ks')");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_ks FOR KEY SHARE")) {
                assertTrue(rs.next());
                assertEquals("ks", rs.getString(2));
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_ks");
            c.commit();
        }
    }

    // --- NOWAIT ---

    @Test void select_for_update_nowait_no_contention() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_nw(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO lock_nw VALUES (1,'nw')");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_nw WHERE id = 1 FOR UPDATE NOWAIT")) {
                assertTrue(rs.next());
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_nw");
            c.commit();
        }
    }

    @Test void select_for_update_nowait_contention_raises_error() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute("CREATE TABLE lock_nw2(id int PRIMARY KEY, v text)");
            c1.createStatement().execute("INSERT INTO lock_nw2 VALUES (1,'x')");
            c1.commit();
            c2.commit();
            // Session 1 locks row
            c1.createStatement().executeQuery("SELECT * FROM lock_nw2 WHERE id = 1 FOR UPDATE");
            // Session 2 tries NOWAIT: should fail immediately with SQLSTATE 55P03
            SQLException ex = assertThrows(SQLException.class, () -> {
                c2.createStatement().executeQuery("SELECT * FROM lock_nw2 WHERE id = 1 FOR UPDATE NOWAIT");
            });
            assertEquals("55P03", ex.getSQLState(), "NOWAIT should raise lock_not_available");
            c1.commit();
            c2.rollback();
            c1.createStatement().execute("DROP TABLE lock_nw2");
            c1.commit();
        }
    }

    // --- SKIP LOCKED ---

    @Test void select_for_update_skip_locked() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute("CREATE TABLE lock_sl(id int PRIMARY KEY, v text)");
            c1.createStatement().execute("INSERT INTO lock_sl VALUES (1,'a'),(2,'b'),(3,'c')");
            c1.commit();
            c2.commit();
            // Session 1 locks row id=2
            c1.createStatement().executeQuery("SELECT * FROM lock_sl WHERE id = 2 FOR UPDATE");
            // Session 2 tries to lock all rows with SKIP LOCKED: should skip row 2
            List<Integer> ids = new ArrayList<>();
            try (ResultSet rs = c2.createStatement().executeQuery(
                    "SELECT * FROM lock_sl ORDER BY id FOR UPDATE SKIP LOCKED")) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            // Row 2 should be skipped
            assertFalse(ids.contains(2), "Locked row should be skipped");
            c1.commit();
            c2.commit();
            c1.createStatement().execute("DROP TABLE lock_sl");
            c1.commit();
        }
    }

    @Test void select_for_update_skip_locked_all_locked_returns_empty() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute("CREATE TABLE lock_sl2(id int PRIMARY KEY)");
            c1.createStatement().execute("INSERT INTO lock_sl2 VALUES (1)");
            c1.commit();
            c2.commit();
            // Lock the only row
            c1.createStatement().executeQuery("SELECT * FROM lock_sl2 FOR UPDATE");
            // SKIP LOCKED should return no rows
            try (ResultSet rs = c2.createStatement().executeQuery(
                    "SELECT * FROM lock_sl2 FOR UPDATE SKIP LOCKED")) {
                assertFalse(rs.next(), "All rows locked, SKIP LOCKED should return empty");
            }
            c1.commit();
            c2.commit();
            c1.createStatement().execute("DROP TABLE lock_sl2");
            c1.commit();
        }
    }

    // --- Lock timeout ---

    @Test void lock_timeout_expires() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute("CREATE TABLE lock_to(id int PRIMARY KEY)");
            c1.createStatement().execute("INSERT INTO lock_to VALUES (1)");
            c1.commit();
            c2.commit();
            c1.createStatement().executeQuery("SELECT * FROM lock_to WHERE id = 1 FOR UPDATE");
            // Set a short lock timeout on session 2
            c2.createStatement().execute("SET lock_timeout = '100ms'");
            long start = System.currentTimeMillis();
            SQLException ex = assertThrows(SQLException.class, () -> {
                c2.createStatement().executeQuery("SELECT * FROM lock_to WHERE id = 1 FOR UPDATE");
            });
            long elapsed = System.currentTimeMillis() - start;
            // PG raises 55P03 for lock_timeout
            assertTrue(ex.getSQLState().equals("55P03") || ex.getSQLState().equals("57014"),
                    "Lock timeout should raise 55P03 or 57014");
            assertTrue(elapsed < 2000,
                    "lock_timeout = 100ms should fail fast, but took " + elapsed + "ms");
            c1.commit();
            c2.rollback();
            c1.createStatement().execute("DROP TABLE lock_to");
            c1.commit();
        }
    }

    // --- Deadlock detection ---

    @Test void deadlock_detection() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute("CREATE TABLE lock_dl(id int PRIMARY KEY, v text)");
            c1.createStatement().execute("INSERT INTO lock_dl VALUES (1,'a'),(2,'b')");
            c1.commit();
            c2.commit();
            // Session 1 locks row 1
            c1.createStatement().executeQuery("SELECT * FROM lock_dl WHERE id = 1 FOR UPDATE");
            // Session 2 locks row 2
            c2.createStatement().executeQuery("SELECT * FROM lock_dl WHERE id = 2 FOR UPDATE");
            // Session 1 tries to lock row 2 (would block)
            // Session 2 tries to lock row 1 (deadlock)
            // Use a thread to simulate concurrency
            ExecutorService ex = Executors.newSingleThreadExecutor();
            Future<?> f = ex.submit(() -> {
                try {
                    c1.createStatement().executeQuery("SELECT * FROM lock_dl WHERE id = 2 FOR UPDATE");
                } catch (SQLException ignored) {}
            });
            Thread.sleep(100);
            // Session 2 tries to lock row 1: should cause deadlock
            try {
                c2.createStatement().executeQuery("SELECT * FROM lock_dl WHERE id = 1 FOR UPDATE");
            } catch (SQLException e) {
                // PG SQLSTATE for deadlock is 40P01
                assertEquals("40P01", e.getSQLState(), "Deadlock should raise 40P01");
            }
            f.cancel(true);
            ex.shutdownNow();
            c1.rollback();
            c2.rollback();
            c1.createStatement().execute("DROP TABLE lock_dl");
            c1.commit();
        }
    }

    // --- Row visibility after lock release ---

    @Test void for_update_sees_committed_changes_after_lock_release() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute("CREATE TABLE lock_vis(id int PRIMARY KEY, v text)");
            c1.createStatement().execute("INSERT INTO lock_vis VALUES (1,'old')");
            c1.commit();
            c2.commit();
            // Session 1 locks and updates
            c1.createStatement().executeQuery("SELECT * FROM lock_vis WHERE id = 1 FOR UPDATE");
            c1.createStatement().execute("UPDATE lock_vis SET v = 'new' WHERE id = 1");
            c1.commit();
            // Session 2 should see the new value
            try (ResultSet rs = c2.createStatement().executeQuery(
                    "SELECT v FROM lock_vis WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("new", rs.getString(1));
            }
            c2.commit();
            c1.createStatement().execute("DROP TABLE lock_vis");
            c1.commit();
        }
    }

    // --- FOR UPDATE with subquery ---

    @Test void select_for_update_with_subquery() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_sq(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO lock_sq VALUES (1,'a'),(2,'b')");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT * FROM lock_sq WHERE id IN (SELECT id FROM lock_sq WHERE v = 'a') FOR UPDATE")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next());
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_sq");
            c.commit();
        }
    }

    // --- FOR UPDATE OF specific table ---

    @Test void select_for_update_of_specific_table() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE lock_of1(id int PRIMARY KEY, v text)");
            c.createStatement().execute("CREATE TABLE lock_of2(id int PRIMARY KEY, ref int)");
            c.createStatement().execute("INSERT INTO lock_of1 VALUES (1,'x')");
            c.createStatement().execute("INSERT INTO lock_of2 VALUES (1,1)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT a.id, a.v FROM lock_of1 a JOIN lock_of2 b ON a.id = b.ref FOR UPDATE OF a")) {
                assertTrue(rs.next());
            }
            c.commit();
            c.createStatement().execute("DROP TABLE lock_of2");
            c.createStatement().execute("DROP TABLE lock_of1");
            c.commit();
        }
    }
}
