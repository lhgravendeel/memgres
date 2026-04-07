package com.memgres.junit5;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests snapshot/restore: data is restored to post-init state before each test.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SnapshotRestoreTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
            .migrationDir("migrations")
            .initScript("test-data.sql")
            .snapshotAfterInit(true)
            .restoreBeforeEach(true)
            .build();

    @Test
    @Order(1)
    void shouldHaveInitialData() throws SQLException {
        try (Connection conn = db.getConnection()) {
            assertEquals(2, countRows(conn, "users"));
            assertEquals(3, countRows(conn, "orders"));
        }
    }

    @Test
    @Order(2)
    void shouldMutateDataDuringTest() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute(
                    "INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')");
            conn.createStatement().execute("DELETE FROM orders");
            assertEquals(3, countRows(conn, "users"));
            assertEquals(0, countRows(conn, "orders"));
        }
    }

    @Test
    @Order(3)
    void shouldSeeRestoredDataAfterMutation() throws SQLException {
        // restore should have rolled back the mutations from test 2
        try (Connection conn = db.getConnection()) {
            assertEquals(2, countRows(conn, "users"));
            assertEquals(3, countRows(conn, "orders"));
        }
    }

    @Test
    @Order(4)
    void shouldPreserveSerialSequences() throws SQLException {
        try (Connection conn = db.getConnection()) {
            // Insert a new user; serial should continue from snapshot value
            conn.createStatement().execute(
                    "INSERT INTO users (name, email) VALUES ('Dave', 'dave@example.com')");
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT id FROM users WHERE name = 'Dave'")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1)); // 1=Alice, 2=Bob, 3=Dave
            }
        }
    }

    private int countRows(Connection conn, String table) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
