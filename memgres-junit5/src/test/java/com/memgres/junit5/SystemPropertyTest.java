package com.memgres.junit5;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that systemProperty sets the JDBC URL as a system property.
 */
class SystemPropertyTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
            .systemProperty("memgres.test.url")
            .build();

    @Test
    void shouldSetSystemProperty() {
        String url = System.getProperty("memgres.test.url");
        assertNotNull(url);
        assertTrue(url.startsWith("jdbc:postgresql://localhost:"));
        assertTrue(url.contains("memgres"));
    }

    @Test
    void shouldBeUsableViaSystemProperty() throws SQLException {
        String url = System.getProperty("memgres.test.url");
        try (Connection conn = DriverManager.getConnection(url, "memgres", "memgres");
             ResultSet rs = conn.createStatement().executeQuery("SELECT 42 AS answer")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }
}
