package com.memgres.junit5;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link MemgresExtension} using @RegisterExtension with
 * migration dir + init script.
 */
class MemgresExtensionTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
            .migrationDir("migrations")
            .initScript("test-data.sql")
            .isolation(IsolationMode.PER_CLASS)
            .build();

    @Test
    void shouldHaveTablesFromMigrations() throws SQLException {
        try (Connection conn = db.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
                     "SELECT COUNT(*) FROM users")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void shouldHaveOrdersFromMigrations() throws SQLException {
        try (Connection conn = db.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
                     "SELECT COUNT(*) FROM orders")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void shouldProvideWorkingDataSource() throws SQLException {
        DataSource ds = db.getDataSource();
        try (Connection conn = ds.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
                     "SELECT name FROM users ORDER BY id LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void shouldProvideJdbcUrl() {
        String url = db.getJdbcUrl();
        assertTrue(url.startsWith("jdbc:postgresql://localhost:"));
        assertFalse(url.contains("preferQueryMode"));
    }

    @Test
    void shouldExposeMemgresInstance() {
        Memgres memgres = db.getMemgres();
        assertNotNull(memgres);
        assertTrue(db.getPort() > 0);
    }

    @Test
    void shouldJoinAcrossMigratedTables() throws SQLException {
        try (Connection conn = db.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
                     "SELECT u.name, SUM(o.amount) as total " +
                             "FROM users u JOIN orders o ON o.user_id = u.id " +
                             "GROUP BY u.name ORDER BY total DESC")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals(79.98, rs.getDouble("total"), 0.01);

            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("name"));
            assertEquals(29.99, rs.getDouble("total"), 0.01);
        }
    }
}
