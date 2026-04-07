package com.memgres.junit5;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the {@link MemgresTest} annotation with parameter injection.
 */
@MemgresTest(
        migrationDirs = "migrations",
        initScripts = "test-data.sql"
)
class MemgresAnnotationTest {

    @Test
    void shouldInjectConnection(Connection conn) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 AS num")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));
        }
    }

    @Test
    void shouldInjectDataSource(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
                     "SELECT COUNT(*) FROM users")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void shouldInjectMemgres(Memgres memgres) {
        assertNotNull(memgres);
        assertTrue(memgres.getPort() > 0);
    }

    @Test
    void shouldHaveTestData(Connection conn) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT name FROM users ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString(1));
        }
    }
}
