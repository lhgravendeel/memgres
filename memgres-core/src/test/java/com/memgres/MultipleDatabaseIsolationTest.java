package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that CREATE DATABASE creates an isolated database within a single
 * Memgres instance, and that JDBC URL routing connects to the correct database.
 */
class MultipleDatabaseIsolationTest {

    @Test
    void tableInOneDatabase_notVisibleInOther() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String baseUrl = "jdbc:postgresql://localhost:" + memgres.getPort();

            // Connect to default database and create a second database
            try (Connection conn = DriverManager.getConnection(
                    baseUrl + "/memgres", memgres.getUser(), memgres.getPassword());
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE db2");
                s.execute("CREATE TABLE default_only (id serial PRIMARY KEY, name text)");
                s.execute("INSERT INTO default_only (name) VALUES ('hello')");
            }

            // Connect to db2 via JDBC URL and verify the table does NOT exist
            try (Connection conn2 = DriverManager.getConnection(
                    baseUrl + "/db2", memgres.getUser(), memgres.getPassword())) {
                conn2.setAutoCommit(true);

                // Verify table does NOT exist in db2
                try (Statement s = conn2.createStatement()) {
                    SQLException ex = assertThrows(SQLException.class,
                            () -> s.executeQuery("SELECT * FROM default_only"));
                    assertEquals("42P01", ex.getSQLState(), "Should be 'relation does not exist'");
                }

                // Verify current_database() returns 'db2'
                try (Statement s = conn2.createStatement();
                     ResultSet rs = s.executeQuery("SELECT current_database()")) {
                    assertTrue(rs.next());
                    assertEquals("db2", rs.getString(1));
                }

                // Create a table in db2 and verify it's only in db2
                try (Statement s = conn2.createStatement()) {
                    s.execute("CREATE TABLE db2_only (id serial PRIMARY KEY, val int)");
                    s.execute("INSERT INTO db2_only (val) VALUES (42)");
                }
            }

            // Verify db2_only does NOT exist in default database
            try (Connection conn = DriverManager.getConnection(
                    baseUrl + "/memgres", memgres.getUser(), memgres.getPassword());
                 Statement s = conn.createStatement()) {
                // default_only should still be there
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM default_only")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
                // db2_only should NOT exist
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT * FROM db2_only"));
                assertEquals("42P01", ex.getSQLState());
            }

            // Connecting to a new database auto-creates it (empty)
            try (Connection conn3 = DriverManager.getConnection(
                    baseUrl + "/auto_created", memgres.getUser(), memgres.getPassword());
                 Statement s = conn3.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("auto_created", rs.getString(1));
            }
        }
    }
}
