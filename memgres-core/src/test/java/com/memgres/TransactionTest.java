package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 10: Transaction Support tests.
 * BEGIN/COMMIT/ROLLBACK, SAVEPOINT, auto-commit, failed transaction state.
 */
class TransactionTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        // Keep autoCommit=true so JDBC doesn't interfere; we control transactions manually
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- Basic COMMIT ----

    @Test
    void testCommitPersistsInserts() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_commit (id INTEGER, name TEXT)");
            stmt.execute("BEGIN");
            stmt.execute("INSERT INTO tx_commit (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO tx_commit (id, name) VALUES (2, 'Bob')");
            stmt.execute("COMMIT");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_commit");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    // ---- Basic ROLLBACK ----

    @Test
    void testRollbackUndoesInserts() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_rb_ins (id INTEGER, name TEXT)");

            stmt.execute("BEGIN");
            stmt.execute("INSERT INTO tx_rb_ins (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO tx_rb_ins (id, name) VALUES (2, 'Bob')");
            stmt.execute("ROLLBACK");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_rb_ins");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void testRollbackUndoesDeletes() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_rb_del (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tx_rb_del (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO tx_rb_del (id, name) VALUES (2, 'Bob')");

            stmt.execute("BEGIN");
            stmt.execute("DELETE FROM tx_rb_del WHERE id = 1");

            // Verify row is deleted during transaction
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_rb_del");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            stmt.execute("ROLLBACK");

            // After rollback, row should be back
            rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_rb_del");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testRollbackUndoesUpdates() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_rb_upd (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tx_rb_upd (id, name) VALUES (1, 'Alice')");

            stmt.execute("BEGIN");
            stmt.execute("UPDATE tx_rb_upd SET name = 'Modified' WHERE id = 1");

            // Verify update is visible during transaction
            ResultSet rs = stmt.executeQuery("SELECT name FROM tx_rb_upd WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Modified", rs.getString("name"));

            stmt.execute("ROLLBACK");

            // After rollback, original value should be restored
            rs = stmt.executeQuery("SELECT name FROM tx_rb_upd WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
        }
    }

    // ---- DDL rollback ----

    @Test
    void testRollbackUndoesCreateTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("BEGIN");
            stmt.execute("CREATE TABLE tx_rb_ct (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tx_rb_ct (id, name) VALUES (1, 'Alice')");
            stmt.execute("ROLLBACK");

            // Table should not exist after rollback
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT COUNT(*) FROM tx_rb_ct"));
        }
    }

    @Test
    void testRollbackUndoesDropTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_rb_dt (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tx_rb_dt (id, name) VALUES (1, 'Alice')");

            stmt.execute("BEGIN");
            stmt.execute("DROP TABLE tx_rb_dt");
            stmt.execute("ROLLBACK");

            // Table should exist after rollback with its data
            ResultSet rs = stmt.executeQuery("SELECT name FROM tx_rb_dt WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
        }
    }

    // ---- SAVEPOINT ----

    @Test
    void testSavepointRollback() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_sp (id INTEGER, name TEXT)");

            stmt.execute("BEGIN");
            stmt.execute("INSERT INTO tx_sp (id, name) VALUES (1, 'Alice')");
            stmt.execute("SAVEPOINT sp1");
            stmt.execute("INSERT INTO tx_sp (id, name) VALUES (2, 'Bob')");
            stmt.execute("INSERT INTO tx_sp (id, name) VALUES (3, 'Charlie')");
            stmt.execute("ROLLBACK TO SAVEPOINT sp1");

            // Only Alice should remain (Bob and Charlie rolled back)
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_sp");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            stmt.execute("COMMIT");

            // After commit, Alice should be persisted
            rs = stmt.executeQuery("SELECT name FROM tx_sp");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testNestedSavepoints() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_nsp (id INTEGER, name TEXT)");

            stmt.execute("BEGIN");
            stmt.execute("INSERT INTO tx_nsp (id, name) VALUES (1, 'A')");
            stmt.execute("SAVEPOINT sp1");
            stmt.execute("INSERT INTO tx_nsp (id, name) VALUES (2, 'B')");
            stmt.execute("SAVEPOINT sp2");
            stmt.execute("INSERT INTO tx_nsp (id, name) VALUES (3, 'C')");

            // Rollback to sp2, removing C
            stmt.execute("ROLLBACK TO SAVEPOINT sp2");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_nsp");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            // Rollback to sp1, removing B
            stmt.execute("ROLLBACK TO SAVEPOINT sp1");
            rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_nsp");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            stmt.execute("COMMIT");

            rs = stmt.executeQuery("SELECT name FROM tx_nsp");
            assertTrue(rs.next());
            assertEquals("A", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testReleaseSavepoint() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_rsp (id INTEGER, name TEXT)");

            stmt.execute("BEGIN");
            stmt.execute("SAVEPOINT sp1");
            stmt.execute("INSERT INTO tx_rsp (id, name) VALUES (1, 'A')");
            stmt.execute("RELEASE SAVEPOINT sp1");

            // After release, can't rollback to sp1
            assertThrows(SQLException.class, () ->
                    stmt.execute("ROLLBACK TO SAVEPOINT sp1"));

            // Transaction is now in FAILED state, rollback entire transaction
            stmt.execute("ROLLBACK");

            // Nothing committed
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_rsp");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    // ---- Failed transaction state ----

    @Test
    void testFailedTransactionRejectsCommands() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_fail (id INTEGER PRIMARY KEY, name TEXT)");

            stmt.execute("BEGIN");
            stmt.execute("INSERT INTO tx_fail (id, name) VALUES (1, 'Alice')");

            // This should fail (duplicate PK)
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO tx_fail (id, name) VALUES (1, 'Duplicate')"));

            // Now any command (except ROLLBACK) should fail with "transaction is aborted"
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT COUNT(*) FROM tx_fail"));
            assertTrue(ex.getMessage().contains("aborted"));

            // ROLLBACK should work
            stmt.execute("ROLLBACK");

            // After rollback, we're back to IDLE and commands work again
            // But the insert was rolled back too
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_fail");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void testFailedTransactionRecoverWithSavepoint() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_fail_sp (id INTEGER PRIMARY KEY, name TEXT)");

            stmt.execute("BEGIN");
            stmt.execute("INSERT INTO tx_fail_sp (id, name) VALUES (1, 'Alice')");
            stmt.execute("SAVEPOINT sp1");

            // This should fail
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO tx_fail_sp (id, name) VALUES (1, 'Duplicate')"));

            // Rollback to savepoint; should recover from FAILED state
            stmt.execute("ROLLBACK TO SAVEPOINT sp1");

            // Now we can continue
            stmt.execute("INSERT INTO tx_fail_sp (id, name) VALUES (2, 'Bob')");
            stmt.execute("COMMIT");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_fail_sp");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    // ---- TRUNCATE rollback ----

    @Test
    void testRollbackUndoesTruncate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_trunc (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tx_trunc (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO tx_trunc (id, name) VALUES (2, 'Bob')");

            stmt.execute("BEGIN");
            stmt.execute("TRUNCATE tx_trunc");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_trunc");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));

            stmt.execute("ROLLBACK");

            rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_trunc");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    // ---- Multiple operations rollback ----

    @Test
    void testRollbackMultipleOperations() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_multi (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tx_multi (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO tx_multi (id, name) VALUES (2, 'Bob')");

            stmt.execute("BEGIN");
            stmt.execute("INSERT INTO tx_multi (id, name) VALUES (3, 'Charlie')");
            stmt.execute("UPDATE tx_multi SET name = 'ALICE' WHERE id = 1");
            stmt.execute("DELETE FROM tx_multi WHERE id = 2");
            stmt.execute("ROLLBACK");

            // Everything should be back to pre-BEGIN state
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM tx_multi ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    // ---- Delete all rows rollback ----

    @Test
    void testRollbackDeleteAll() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tx_del_all (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tx_del_all (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO tx_del_all (id, name) VALUES (2, 'Bob')");

            stmt.execute("BEGIN");
            stmt.execute("DELETE FROM tx_del_all");
            stmt.execute("ROLLBACK");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tx_del_all");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    // ---- Sequence rollback ----

    @Test
    void testRollbackUndoesCreateSequence() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("BEGIN");
            stmt.execute("CREATE SEQUENCE tx_seq START WITH 1");
            ResultSet rs = stmt.executeQuery("SELECT nextval('tx_seq')");
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            stmt.execute("ROLLBACK");

            // Sequence should not exist after rollback
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT nextval('tx_seq')"));
        }
    }
}
