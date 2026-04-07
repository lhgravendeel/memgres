package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 10: Crash-recovery / durability-related behavior.
 * Tests unlogged tables, temporary objects after reconnect,
 * sequence state after rollback.
 */
class CrashRecoveryDurabilityTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // --- Unlogged tables ---

    @Test void unlogged_table_creation() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE UNLOGGED TABLE unlog_t(id int PRIMARY KEY, v text)");
            c.createStatement().execute("INSERT INTO unlog_t VALUES (1,'a')");
            try (ResultSet rs = c.createStatement().executeQuery("SELECT v FROM unlog_t WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
            }
            c.createStatement().execute("DROP TABLE unlog_t");
        }
    }

    @Test void unlogged_table_with_index() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE UNLOGGED TABLE unlog_idx(id int PRIMARY KEY, v text)");
            c.createStatement().execute("CREATE INDEX unlog_idx_v ON unlog_idx(v)");
            c.createStatement().execute("INSERT INTO unlog_idx VALUES (1,'a'),(2,'b')");
            try (ResultSet rs = c.createStatement().executeQuery("SELECT id FROM unlog_idx WHERE v = 'b'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            c.createStatement().execute("DROP TABLE unlog_idx");
        }
    }

    // --- Temporary objects after reconnect ---

    @Test void temp_table_not_visible_in_other_session() throws Exception {
        try (Connection c1 = newConn()) {
            c1.createStatement().execute("CREATE TEMP TABLE temp_sess(id int)");
            c1.createStatement().execute("INSERT INTO temp_sess VALUES (1)");
            // Open new connection: temp table should not be visible
            try (Connection c2 = newConn()) {
                assertThrows(SQLException.class, () -> {
                    c2.createStatement().executeQuery("SELECT * FROM temp_sess");
                });
            }
            c1.createStatement().execute("DROP TABLE temp_sess");
        }
    }

    @Test void temp_table_gone_after_disconnect() throws Exception {
        String tableName = "temp_disc_" + System.currentTimeMillis();
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TEMP TABLE " + tableName + "(id int)");
            c.createStatement().execute("INSERT INTO " + tableName + " VALUES (1)");
        }
        // Reconnect: table should be gone
        try (Connection c = newConn()) {
            assertThrows(SQLException.class, () -> {
                c.createStatement().executeQuery("SELECT * FROM " + tableName);
            });
        }
    }

    @Test void temp_sequence_not_visible_in_other_session() throws Exception {
        try (Connection c1 = newConn()) {
            c1.createStatement().execute("CREATE TEMP SEQUENCE temp_seq_s");
            try (ResultSet rs = c1.createStatement().executeQuery("SELECT nextval('temp_seq_s')")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));
            }
            // Other session can't see it
            try (Connection c2 = newConn()) {
                assertThrows(SQLException.class, () -> {
                    c2.createStatement().executeQuery("SELECT nextval('temp_seq_s')");
                });
            }
        }
    }

    // --- Sequence state after rollback ---

    @Test void sequence_advances_even_after_rollback() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(false);
            c.createStatement().execute("CREATE SEQUENCE seq_rollback");
            c.commit();
            // Advance the sequence
            try (ResultSet rs = c.createStatement().executeQuery("SELECT nextval('seq_rollback')")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));
            }
            c.rollback();
            // After rollback, sequence should still be at 1 (PG sequences don't roll back)
            try (ResultSet rs = c.createStatement().executeQuery("SELECT nextval('seq_rollback')")) {
                assertTrue(rs.next());
                long val = rs.getLong(1);
                // PG: nextval after rollback returns 2 (sequences never roll back)
                assertTrue(val >= 2, "Sequence should advance past rolled-back value, got: " + val);
            }
            c.commit();
            c.createStatement().execute("DROP SEQUENCE seq_rollback");
            c.commit();
        }
    }

    @Test void serial_sequence_after_rolled_back_insert() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(false);
            c.createStatement().execute("CREATE TABLE seq_rb_t(id serial PRIMARY KEY, v text)");
            c.commit();
            c.createStatement().execute("INSERT INTO seq_rb_t(v) VALUES ('a')"); // id=1
            c.rollback();
            c.createStatement().execute("INSERT INTO seq_rb_t(v) VALUES ('b')"); // id should be 2, not 1
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT id FROM seq_rb_t")) {
                assertTrue(rs.next());
                long id = rs.getLong(1);
                assertTrue(id >= 2, "ID after rollback should skip: " + id);
            }
            c.createStatement().execute("DROP TABLE seq_rb_t");
            c.commit();
        }
    }
}
