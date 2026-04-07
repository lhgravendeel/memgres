package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that starts Memgres and connects via the PostgreSQL JDBC driver.
 */
class MemgresTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) {
            memgres.close();
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl(),
                memgres.getUser(),
                memgres.getPassword()
        );
    }

    @Test
    void shouldConnectAndRunSimpleQuery() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS num")) {
            assertTrue(rs.next());
            assertEquals("1", rs.getString("num"));
        }
    }

    @Test
    void shouldCreateTableAndInsertData() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL, email varchar)");
            stmt.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");
            stmt.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM users")) {
                assertTrue(rs.next());
                assertEquals("Alice", rs.getString("name"));
                assertEquals("alice@example.com", rs.getString("email"));

                assertTrue(rs.next());
                assertEquals("Bob", rs.getString("name"));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldDeleteRows() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");

            stmt.execute("DELETE FROM items WHERE id = 2");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
                assertTrue(rs.next());
                assertEquals("apple", rs.getString("name"));
                assertTrue(rs.next());
                assertEquals("cherry", rs.getString("name"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldUpdateRows() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE products (id integer, price integer)");
            stmt.execute("INSERT INTO products (id, price) VALUES (1, 100)");
            stmt.execute("INSERT INTO products (id, price) VALUES (2, 200)");

            stmt.execute("UPDATE products SET price = 150 WHERE id = 1");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM products WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("150", rs.getString("price"));
            }
        }
    }

    @Test
    void shouldHandleDropTable() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE temp_table (id integer)");
            stmt.execute("DROP TABLE temp_table");

            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT * FROM temp_table"));
        }
    }

    @Test
    void shouldReportVersion() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT version()")) {
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains("PostgreSQL"));
        }
    }
}
