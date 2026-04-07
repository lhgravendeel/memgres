package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge case tests for index maintenance during UPDATE operations.
 * Focuses on scenarios where multiple rows are affected and index
 * entries must be correctly added/removed around in-place mutations.
 */
class IndexUpdateEdgeCaseTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- Update one row, verify other row's index entry survives ----

    @Test
    void updateOneRowOtherStillProtected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue1 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_ue1 VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC')");
            // Update only row 2
            s.execute("UPDATE idx_ue1 SET code = 'XXX' WHERE id = 2");
            // AAA and CCC should still be protected
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue1 VALUES (4, 'AAA')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue1 VALUES (4, 'CCC')"));
            // BBB should be free, XXX should be taken
            s.execute("INSERT INTO idx_ue1 VALUES (4, 'BBB')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue1 VALUES (5, 'XXX')"));
        }
    }

    // ---- Update non-indexed column: index should be unchanged ----

    @Test
    void updateNonIndexedColumnIndexUnchanged() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue2 (id INTEGER PRIMARY KEY, code TEXT UNIQUE, data TEXT)");
            s.execute("INSERT INTO idx_ue2 VALUES (1, 'A', 'old')");
            s.execute("UPDATE idx_ue2 SET data = 'new' WHERE id = 1");
            // PK and UNIQUE should still work
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue2 VALUES (1, 'B', 'x')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue2 VALUES (2, 'A', 'x')"));
        }
    }

    // ---- Self-update (SET col = col), index should survive ----

    @Test
    void selfUpdateIndexSurvives() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue3 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_ue3 VALUES (1, 'A'), (2, 'B')");
            // No-op update
            s.execute("UPDATE idx_ue3 SET code = code");
            // Both should still be protected
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue3 VALUES (3, 'A')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue3 VALUES (3, 'B')"));
        }
    }

    @Test
    void selfUpdatePkIndexSurvives() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue3b (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_ue3b VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s.execute("UPDATE idx_ue3b SET id = id WHERE val = 'b'");
            // All PKs still protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue3b VALUES (1, 'x')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue3b VALUES (2, 'x')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue3b VALUES (3, 'x')"));
        }
    }

    // ---- Batch update: multiple rows change indexed values ----

    @Test
    void batchUpdateUniqueColumnNonConflicting() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue4 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_ue4 VALUES (1, 'A'), (2, 'B'), (3, 'C')");
            // Update all to prefixed versions; no conflicts among new values
            s.execute("UPDATE idx_ue4 SET code = 'X_' || code");
            // Old values should be free
            s.execute("INSERT INTO idx_ue4 VALUES (4, 'A')");
            s.execute("INSERT INTO idx_ue4 VALUES (5, 'B')");
            s.execute("INSERT INTO idx_ue4 VALUES (6, 'C')");
            // New values should be taken
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue4 VALUES (7, 'X_A')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue4 VALUES (7, 'X_B')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue4 VALUES (7, 'X_C')"));
        }
    }

    // ---- Update that would create conflict with existing row ----

    @Test
    void updateCreatesConflictWithExistingRow() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue5 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_ue5 VALUES (1, 'A'), (2, 'B')");
            // Try to set row 1's code to 'B', which should conflict with row 2
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE idx_ue5 SET code = 'B' WHERE id = 1"));
            // After the failed update, index should be intact
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue5 VALUES (3, 'A')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue5 VALUES (3, 'B')"));
            // Verify data unchanged
            ResultSet rs = s.executeQuery("SELECT code FROM idx_ue5 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
        }
    }

    // ---- Update PK to value freed by same batch (shift-up pattern) ----

    @Test
    void updatePkShiftUp() throws SQLException {
        // In PG: UPDATE t SET id = id + 10 WHERE id IN (1,2,3) works if no conflicts
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue6 (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_ue6 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            // Shift all IDs up by 10, no overlap
            s.execute("UPDATE idx_ue6 SET id = id + 10");
            // Old IDs should be free
            s.execute("INSERT INTO idx_ue6 VALUES (1, 'new1')");
            s.execute("INSERT INTO idx_ue6 VALUES (2, 'new2')");
            s.execute("INSERT INTO idx_ue6 VALUES (3, 'new3')");
            // New IDs should be taken
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue6 VALUES (11, 'dup')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue6 VALUES (12, 'dup')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue6 VALUES (13, 'dup')"));
        }
    }

    // ---- Update with composite PK ----

    @Test
    void updateCompositeKeyOneColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue7 (a INTEGER, b TEXT, val TEXT, PRIMARY KEY (a, b))");
            s.execute("INSERT INTO idx_ue7 VALUES (1, 'x', 'v1'), (1, 'y', 'v2'), (2, 'x', 'v3')");
            // Update b for (1, 'x') to (1, 'z')
            s.execute("UPDATE idx_ue7 SET b = 'z' WHERE a = 1 AND b = 'x'");
            // (1, 'x') should be free
            s.execute("INSERT INTO idx_ue7 VALUES (1, 'x', 'reused')");
            // (1, 'z') should be taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue7 VALUES (1, 'z', 'dup')"));
            // (1, 'y') and (2, 'x') still protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue7 VALUES (1, 'y', 'dup')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue7 VALUES (2, 'x', 'dup')"));
        }
    }

    // ---- Failed update leaves index intact ----

    @Test
    void failedUpdateCheckConstraintIndexIntact() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue8 (id INTEGER PRIMARY KEY, val INTEGER CHECK (val > 0))");
            s.execute("INSERT INTO idx_ue8 VALUES (1, 10), (2, 20)");
            // This update should fail on CHECK constraint
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE idx_ue8 SET val = -1 WHERE id = 1"));
            // Index should be intact after failed update
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue8 VALUES (1, 5)"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue8 VALUES (2, 5)"));
        }
    }

    @Test
    void failedUpdateFkViolationIndexIntact() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue9p (id INTEGER PRIMARY KEY)");
            s.execute("CREATE TABLE idx_ue9c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES idx_ue9p(id))");
            s.execute("INSERT INTO idx_ue9p VALUES (1), (2)");
            s.execute("INSERT INTO idx_ue9c VALUES (10, 1), (20, 2)");
            // Try to update FK to nonexistent parent
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE idx_ue9c SET pid = 999 WHERE id = 10"));
            // Index should be intact
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue9c VALUES (10, 1)"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue9c VALUES (20, 2)"));
        }
    }

    // ---- Update with multiple unique constraints ----

    @Test
    void updateWithMultipleUniqueConstraints() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue10 (id INTEGER PRIMARY KEY, email TEXT UNIQUE, badge INTEGER UNIQUE)");
            s.execute("INSERT INTO idx_ue10 VALUES (1, 'alice@t.com', 100)");
            s.execute("INSERT INTO idx_ue10 VALUES (2, 'bob@t.com', 200)");
            // Update email only
            s.execute("UPDATE idx_ue10 SET email = 'alice2@t.com' WHERE id = 1");
            // Old email free, new email taken, badge unchanged
            s.execute("INSERT INTO idx_ue10 VALUES (3, 'alice@t.com', 300)"); // old email reusable
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue10 VALUES (4, 'alice2@t.com', 400)")); // new email taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue10 VALUES (4, 'new@t.com', 100)")); // badge still taken
        }
    }

    // ---- Repeated updates to same row ----

    @Test
    void repeatedUpdatesToSameRow() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue11 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_ue11 VALUES (1, 'V1')");
            s.execute("INSERT INTO idx_ue11 VALUES (2, 'OTHER')");
            // Update same row multiple times
            s.execute("UPDATE idx_ue11 SET code = 'V2' WHERE id = 1");
            s.execute("UPDATE idx_ue11 SET code = 'V3' WHERE id = 1");
            s.execute("UPDATE idx_ue11 SET code = 'V4' WHERE id = 1");
            // Only V4 and OTHER should be taken
            s.execute("INSERT INTO idx_ue11 VALUES (3, 'V1')"); // old values free
            s.execute("INSERT INTO idx_ue11 VALUES (4, 'V2')");
            s.execute("INSERT INTO idx_ue11 VALUES (5, 'V3')");
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue11 VALUES (6, 'V4')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue11 VALUES (6, 'OTHER')"));
        }
    }

    // ---- Update via prepared statement in batch ----

    @Test
    void preparedStatementBatchUpdate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue12 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            for (int i = 1; i <= 100; i++) {
                s.execute("INSERT INTO idx_ue12 VALUES (" + i + ", 'C" + i + "')");
            }
        }
        // Batch update: rename all codes
        try (PreparedStatement ps = conn.prepareStatement("UPDATE idx_ue12 SET code = ? WHERE id = ?")) {
            for (int i = 1; i <= 100; i++) {
                ps.setString(1, "NEW_" + i);
                ps.setInt(2, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        // All old codes free, all new codes taken
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO idx_ue12 VALUES (101, 'C1')");   // old code reusable
            s.execute("INSERT INTO idx_ue12 VALUES (102, 'C50')");  // old code reusable
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue12 VALUES (200, 'NEW_1')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ue12 VALUES (200, 'NEW_50')"));
        }
    }

    // ---- Interleaved insert and update ----

    @Test
    void interleavedInsertUpdateDelete() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue13 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_ue13 VALUES (1, 'A')");
            s.execute("UPDATE idx_ue13 SET code = 'B' WHERE id = 1");
            s.execute("INSERT INTO idx_ue13 VALUES (2, 'A')"); // A is free now
            s.execute("DELETE FROM idx_ue13 WHERE id = 1");     // B freed
            s.execute("INSERT INTO idx_ue13 VALUES (3, 'B')"); // B reusable
            s.execute("UPDATE idx_ue13 SET code = 'C' WHERE id = 2"); // A freed
            s.execute("INSERT INTO idx_ue13 VALUES (4, 'A')"); // A reusable

            ResultSet rs = s.executeQuery("SELECT id, code FROM idx_ue13 ORDER BY id");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("C", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("B", rs.getString(2));
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1)); assertEquals("A", rs.getString(2));
            assertFalse(rs.next());

            // All current values protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue13 VALUES (5, 'A')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue13 VALUES (5, 'B')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue13 VALUES (5, 'C')"));
        }
    }

    // ---- Update + ON CONFLICT interplay ----

    @Test
    void updateThenOnConflict() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue14 (id INTEGER PRIMARY KEY, code TEXT UNIQUE, ver INTEGER DEFAULT 1)");
            s.execute("INSERT INTO idx_ue14 VALUES (1, 'A', 1)");
            // Update code
            s.execute("UPDATE idx_ue14 SET code = 'B' WHERE id = 1");
            // ON CONFLICT on old code 'A': should insert (no conflict)
            s.execute("INSERT INTO idx_ue14 VALUES (2, 'A', 1) ON CONFLICT (code) DO UPDATE SET ver = idx_ue14.ver + 1");
            // ON CONFLICT on new code 'B': should upsert
            s.execute("INSERT INTO idx_ue14 VALUES (3, 'B', 1) ON CONFLICT (code) DO UPDATE SET ver = idx_ue14.ver + 1");
            ResultSet rs = s.executeQuery("SELECT id, code, ver FROM idx_ue14 ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("B", rs.getString(2)); assertEquals(2, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("A", rs.getString(2)); assertEquals(1, rs.getInt(3));
            assertFalse(rs.next());
        }
    }

    // ---- Rollback of batch update ----

    @Test
    void rollbackBatchUpdateRestoresIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ue15 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_ue15 VALUES (1, 'A'), (2, 'B'), (3, 'C')");
            conn.setAutoCommit(false);
            s.execute("UPDATE idx_ue15 SET code = code || '_new'");
            // Within transaction, old values should be free
            s.execute("INSERT INTO idx_ue15 VALUES (4, 'A')");
            conn.rollback();
            conn.setAutoCommit(true);
            // After rollback, A, B, C should be taken again
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue15 VALUES (4, 'A')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue15 VALUES (4, 'B')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_ue15 VALUES (4, 'C')"));
            // _new values should be free
            s.execute("INSERT INTO idx_ue15 VALUES (4, 'A_new')");
        }
    }
}
