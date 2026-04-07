package com.memgres.junit5;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests GLOBAL isolation mode with snapshot/restore.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GlobalIsolationTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
            .initStatements(
                    "CREATE TABLE global_test (id SERIAL PRIMARY KEY, val TEXT)",
                    "INSERT INTO global_test (val) VALUES ('seed')")
            .isolation(IsolationMode.GLOBAL)
            .snapshotAfterInit(true)
            .restoreBeforeEach(true)
            .build();

    // Remember the port to verify same instance is reused
    private static int firstPort;

    @AfterAll
    static void cleanup() {
        // Clean up global state so other test classes aren't affected
        MemgresExtension.resetGlobalState();
    }

    @Test
    @Order(1)
    void shouldStartGlobalInstance() {
        firstPort = db.getPort();
        assertTrue(firstPort > 0);
    }

    @Test
    @Order(2)
    void shouldReuseGlobalInstance() {
        assertEquals(firstPort, db.getPort(), "Should reuse the same Memgres instance");
    }

    @Test
    @Order(3)
    void shouldMutateAndSeeRestoredData() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute("INSERT INTO global_test (val) VALUES ('extra')");
            assertEquals(2, countRows(conn));
        }
    }

    @Test
    @Order(4)
    void shouldSeeOnlySeedDataAfterRestore() throws SQLException {
        try (Connection conn = db.getConnection()) {
            assertEquals(1, countRows(conn));
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT val FROM global_test")) {
                assertTrue(rs.next());
                assertEquals("seed", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    private int countRows(Connection conn) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT COUNT(*) FROM global_test")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
