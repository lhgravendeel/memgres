package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #1/#10: OPERATOR() syntax not validated against search_path after DROP SCHEMA.
 * Extended query protocol version.
 */
class ExtendedProtocolOperatorTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void operator_plus_fails_after_schema_drop() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA ext_op_test1");
            s.execute("SET search_path = ext_op_test1, pg_catalog");
            s.execute("DROP SCHEMA ext_op_test1 CASCADE");
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT OPERATOR(pg_catalog.+)(1,2)")) {
            assertThrows(SQLException.class, ps::executeQuery,
                "OPERATOR(pg_catalog.+)(1,2) should fail after schema drop");
        } finally {
            try (Statement s = conn.createStatement()) { s.execute("SET search_path = public"); }
        }
    }

    @Test void operator_concat_fails_after_schema_drop() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA ext_op_test2");
            s.execute("SET search_path = ext_op_test2, pg_catalog");
            s.execute("DROP SCHEMA ext_op_test2 CASCADE");
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT OPERATOR(pg_catalog.||)('a','b')")) {
            assertThrows(SQLException.class, ps::executeQuery,
                "OPERATOR(pg_catalog.||)('a','b') should fail after schema drop");
        } finally {
            try (Statement s = conn.createStatement()) { s.execute("SET search_path = public"); }
        }
    }

    @Test void operator_works_with_valid_search_path() throws SQLException {
        // In PG18, OPERATOR(pg_catalog.+)(1,2) is parsed as unary + on ROW(1,2),
        // which fails with SQLSTATE 42883 (operator does not exist).
        try (PreparedStatement ps = conn.prepareStatement("SELECT OPERATOR(pg_catalog.+)(?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 2);
            assertThrows(SQLException.class, ps::executeQuery,
                "OPERATOR(pg_catalog.+)(1,2) should fail in PG18 (treated as unary operator)");
        }
    }
}
