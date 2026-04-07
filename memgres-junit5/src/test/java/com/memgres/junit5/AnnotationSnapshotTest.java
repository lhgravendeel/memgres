package com.memgres.junit5;

import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the annotation-driven snapshotAfterInit option.
 */
@MemgresTest(
        migrationDirs = "migrations",
        initScripts = "test-data.sql",
        snapshotAfterInit = true
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AnnotationSnapshotTest {

    @Test
    @Order(1)
    void shouldDeleteAllOrders(Connection conn) throws SQLException {
        conn.createStatement().execute("DELETE FROM orders");
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM orders")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    @Order(2)
    void shouldHaveOrdersRestoredFromSnapshot(Connection conn) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM orders")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }
}
