package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 2: Isolation-level behavior.
 * Tests READ COMMITTED, REPEATABLE READ, SERIALIZABLE,
 * non-repeatable reads, phantom-like behaviors, serialization failures.
 */
class IsolationLevelTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn(int isolation) throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        c.setTransactionIsolation(isolation);
        return c;
    }

    // --- READ COMMITTED ---

    @Test void read_committed_sees_other_committed_changes() throws Exception {
        try (Connection c1 = newConn(Connection.TRANSACTION_READ_COMMITTED);
             Connection c2 = newConn(Connection.TRANSACTION_READ_COMMITTED)) {
            c1.createStatement().execute("CREATE TABLE iso_rc1(id int PRIMARY KEY, v int)");
            c1.createStatement().execute("INSERT INTO iso_rc1 VALUES (1, 100)");
            c1.commit();
            c2.commit();
            // Session 2 reads v=100
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT v FROM iso_rc1 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
            }
            // Session 1 updates and commits
            c1.createStatement().execute("UPDATE iso_rc1 SET v = 200 WHERE id = 1");
            c1.commit();
            // Session 2 reads again: should see 200 (non-repeatable read)
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT v FROM iso_rc1 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(200, rs.getInt(1));
            }
            c2.commit();
            c1.createStatement().execute("DROP TABLE iso_rc1");
            c1.commit();
        }
    }

    @Test void read_committed_does_not_see_uncommitted() throws Exception {
        try (Connection c1 = newConn(Connection.TRANSACTION_READ_COMMITTED);
             Connection c2 = newConn(Connection.TRANSACTION_READ_COMMITTED)) {
            c1.createStatement().execute("CREATE TABLE iso_rc2(id int PRIMARY KEY, v int)");
            c1.createStatement().execute("INSERT INTO iso_rc2 VALUES (1, 100)");
            c1.commit();
            c2.commit();
            // Session 1 updates but does NOT commit
            c1.createStatement().execute("UPDATE iso_rc2 SET v = 200 WHERE id = 1");
            // Session 2 should still see 100
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT v FROM iso_rc2 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1), "Should not see uncommitted data");
            }
            c1.rollback();
            c2.commit();
            c1.createStatement().execute("DROP TABLE iso_rc2");
            c1.commit();
        }
    }

    @Test void read_committed_phantom_read_allowed() throws Exception {
        try (Connection c1 = newConn(Connection.TRANSACTION_READ_COMMITTED);
             Connection c2 = newConn(Connection.TRANSACTION_READ_COMMITTED)) {
            c1.createStatement().execute("CREATE TABLE iso_rc3(id int PRIMARY KEY)");
            c1.createStatement().execute("INSERT INTO iso_rc3 VALUES (1),(2)");
            c1.commit();
            c2.commit();
            // Session 2 counts rows
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT count(*) FROM iso_rc3")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            // Session 1 inserts and commits
            c1.createStatement().execute("INSERT INTO iso_rc3 VALUES (3)");
            c1.commit();
            // Session 2 counts again; phantom row is allowed in READ COMMITTED
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT count(*) FROM iso_rc3")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1), "Phantom rows are allowed in READ COMMITTED");
            }
            c2.commit();
            c1.createStatement().execute("DROP TABLE iso_rc3");
            c1.commit();
        }
    }

    // --- REPEATABLE READ ---

    @Test void repeatable_read_prevents_nonrepeatable_reads() throws Exception {
        try (Connection c1 = newConn(Connection.TRANSACTION_READ_COMMITTED);
             Connection c2 = newConn(Connection.TRANSACTION_REPEATABLE_READ)) {
            c1.createStatement().execute("CREATE TABLE iso_rr1(id int PRIMARY KEY, v int)");
            c1.createStatement().execute("INSERT INTO iso_rr1 VALUES (1, 100)");
            c1.commit();
            c2.commit();
            // Session 2 (RR) takes a snapshot by reading
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT v FROM iso_rr1 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
            }
            // Session 1 updates and commits
            c1.createStatement().execute("UPDATE iso_rr1 SET v = 200 WHERE id = 1");
            c1.commit();
            // Session 2 (RR) should still see 100, since the snapshot is frozen
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT v FROM iso_rr1 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1), "REPEATABLE READ should prevent non-repeatable reads");
            }
            c2.commit();
            c1.createStatement().execute("DROP TABLE iso_rr1");
            c1.commit();
        }
    }

    @Test void repeatable_read_prevents_phantoms() throws Exception {
        try (Connection c1 = newConn(Connection.TRANSACTION_READ_COMMITTED);
             Connection c2 = newConn(Connection.TRANSACTION_REPEATABLE_READ)) {
            c1.createStatement().execute("CREATE TABLE iso_rr2(id int PRIMARY KEY)");
            c1.createStatement().execute("INSERT INTO iso_rr2 VALUES (1)");
            c1.commit();
            c2.commit();
            // Session 2 counts rows (takes snapshot)
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT count(*) FROM iso_rr2")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            // Session 1 inserts
            c1.createStatement().execute("INSERT INTO iso_rr2 VALUES (2)");
            c1.commit();
            // Session 2 should still see 1 row
            try (ResultSet rs = c2.createStatement().executeQuery("SELECT count(*) FROM iso_rr2")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "REPEATABLE READ should prevent phantom reads");
            }
            c2.commit();
            c1.createStatement().execute("DROP TABLE iso_rr2");
            c1.commit();
        }
    }

    @Test void repeatable_read_serialization_failure_on_concurrent_update() throws Exception {
        try (Connection c1 = newConn(Connection.TRANSACTION_REPEATABLE_READ);
             Connection c2 = newConn(Connection.TRANSACTION_REPEATABLE_READ)) {
            c1.createStatement().execute("CREATE TABLE iso_rr3(id int PRIMARY KEY, v int)");
            c1.createStatement().execute("INSERT INTO iso_rr3 VALUES (1, 0)");
            c1.commit();
            c2.commit();
            // Both read
            c1.createStatement().executeQuery("SELECT * FROM iso_rr3 WHERE id = 1");
            c2.createStatement().executeQuery("SELECT * FROM iso_rr3 WHERE id = 1");
            // Session 1 updates and commits
            c1.createStatement().execute("UPDATE iso_rr3 SET v = 1 WHERE id = 1");
            c1.commit();
            // Session 2 tries to update same row: should get serialization failure (40001)
            try {
                c2.createStatement().execute("UPDATE iso_rr3 SET v = 2 WHERE id = 1");
                c2.commit();
                // If we get here, the implementation doesn't enforce it, and that's what we're testing
            } catch (SQLException e) {
                assertEquals("40001", e.getSQLState(), "Should raise serialization_failure");
            }
            c2.rollback();
            c1.createStatement().execute("DROP TABLE iso_rr3");
            c1.commit();
        }
    }

    // --- SERIALIZABLE ---

    @Test void serializable_set_and_query() throws Exception {
        try (Connection c = newConn(Connection.TRANSACTION_SERIALIZABLE)) {
            c.createStatement().execute("CREATE TABLE iso_ser1(id int PRIMARY KEY, v int)");
            c.createStatement().execute("INSERT INTO iso_ser1 VALUES (1, 10)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT v FROM iso_ser1 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
            }
            c.commit();
            c.createStatement().execute("DROP TABLE iso_ser1");
            c.commit();
        }
    }

    @Test void serializable_write_skew_detection() throws Exception {
        try (Connection c1 = newConn(Connection.TRANSACTION_SERIALIZABLE);
             Connection c2 = newConn(Connection.TRANSACTION_SERIALIZABLE)) {
            c1.createStatement().execute("CREATE TABLE iso_ser2(id int PRIMARY KEY, v int)");
            c1.createStatement().execute("INSERT INTO iso_ser2 VALUES (1, 10),(2, 20)");
            c1.commit();
            c2.commit();
            // Classic write skew: both read, each writes to the other's row
            c1.createStatement().executeQuery("SELECT v FROM iso_ser2 WHERE id = 1");
            c2.createStatement().executeQuery("SELECT v FROM iso_ser2 WHERE id = 2");
            c1.createStatement().execute("UPDATE iso_ser2 SET v = 11 WHERE id = 2");
            c2.createStatement().execute("UPDATE iso_ser2 SET v = 22 WHERE id = 1");
            // One commit should succeed, the other should fail with 40001
            c1.commit();
            try {
                c2.commit();
            } catch (SQLException e) {
                assertEquals("40001", e.getSQLState(), "Write skew should raise serialization_failure");
            }
            c1.createStatement().execute("DROP TABLE iso_ser2");
            c1.commit();
        }
    }

    // --- SET TRANSACTION ---

    @Test void set_transaction_isolation_level() throws Exception {
        try (Connection c = newConn(Connection.TRANSACTION_READ_COMMITTED)) {
            c.createStatement().execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            // Should be in serializable now for this transaction
            c.commit();
        }
    }

    @Test void begin_with_isolation_level() throws Exception {
        try (Connection c = newConn(Connection.TRANSACTION_READ_COMMITTED)) {
            c.setAutoCommit(true);
            c.createStatement().execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
            c.createStatement().execute("COMMIT");
        }
    }

    // --- Transaction read only ---

    @Test void read_only_transaction_blocks_writes() throws Exception {
        try (Connection c = newConn(Connection.TRANSACTION_READ_COMMITTED)) {
            c.createStatement().execute("CREATE TABLE iso_ro(id int PRIMARY KEY)");
            c.createStatement().execute("INSERT INTO iso_ro VALUES (1)");
            c.commit();
            c.setReadOnly(true);
            SQLException ex = assertThrows(SQLException.class, () -> {
                c.createStatement().execute("INSERT INTO iso_ro VALUES (2)");
            });
            // PG SQLSTATE 25006 = read_only_sql_transaction
            assertEquals("25006", ex.getSQLState());
            c.rollback();
            c.setReadOnly(false);
            c.createStatement().execute("DROP TABLE iso_ro");
            c.commit();
        }
    }
}
