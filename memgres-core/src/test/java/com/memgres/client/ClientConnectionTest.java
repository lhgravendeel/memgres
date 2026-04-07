package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the exact queries a database client sends on connection.
 * Issue: SET application_name = '' fails with syntax error.
 */
class ClientConnectionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Use default mode (extended protocol) to match typical database clients
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    @Test void set_extra_float_digits() throws SQLException {
        exec("SET extra_float_digits = 3");
    }

    @Test void set_application_name_empty_string() throws SQLException {
        // PG accepts: SET application_name = ''  (empty string)
        exec("SET application_name = ''");
    }

    @Test void set_application_name_nonempty() throws SQLException {
        exec("SET application_name = 'test_client'");
    }

    // Also test via simple query mode
    @Test void set_application_name_empty_simple_mode() throws SQLException {
        try (Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            try (Statement s = c.createStatement()) {
                s.execute("SET application_name = ''");
            }
        }
    }

    // Full client connection sequence
    @Test void full_client_sequence() throws SQLException {
        exec("SET extra_float_digits = 3");
        exec("SET application_name = ''");
    }
}
