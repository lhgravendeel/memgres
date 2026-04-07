package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #25, #26, #27: EXPLAIN with bad options returns 22023 in memgres but
 * PG returns 42P01 (undefined_table) because it checks table existence before
 * validating EXPLAIN options.
 *
 * After DROP SCHEMA, admin_t no longer exists. PG errors with 42P01. Memgres
 * validates the EXPLAIN option first and returns 22023.
 *
 * Fix: memgres should check table existence before option validation,
 * OR return the correct SQLSTATE for genuinely invalid options.
 */
class ExplainOptionSqlstateTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Diff #25: EXPLAIN (FORMAT yamlish) on nonexistent table → PG returns 42P01
    @Test void explain_bad_format_nonexistent_table_sqlstate() throws SQLException {
        // Ensure no_such_table doesn't exist
        try {
            exec("EXPLAIN (FORMAT yamlish) SELECT * FROM no_such_table_explain");
            fail("Should fail");
        } catch (SQLException e) {
            assertEquals("42P01", e.getSQLState(),
                "EXPLAIN on nonexistent table should be 42P01 (even with bad FORMAT), got " + e.getSQLState());
        }
    }

    // Diff #26: EXPLAIN (ANALYZE maybe) on nonexistent table → PG returns 42P01
    @Test void explain_bad_analyze_nonexistent_table_sqlstate() throws SQLException {
        try {
            exec("EXPLAIN (ANALYZE maybe) SELECT * FROM no_such_table_explain");
            fail("Should fail");
        } catch (SQLException e) {
            assertEquals("42P01", e.getSQLState(),
                "Should be 42P01, got " + e.getSQLState());
        }
    }

    // Diff #27: EXPLAIN (BUFFERS 123) on nonexistent table → PG returns 42P01
    @Test void explain_bad_buffers_nonexistent_table_sqlstate() throws SQLException {
        try {
            exec("EXPLAIN (BUFFERS 123) SELECT * FROM no_such_table_explain");
            fail("Should fail");
        } catch (SQLException e) {
            assertEquals("42P01", e.getSQLState(),
                "Should be 42P01, got " + e.getSQLState());
        }
    }
}
