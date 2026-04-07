package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #7, #10, #23: OPERATOR(pg_catalog.+)(1,2) and OPERATOR(pg_catalog.||)('a','b')
 * succeed in memgres but fail in PG after DROP SCHEMA CASCADE.
 * The OPERATOR() syntax should do proper schema-qualified operator lookup.
 */
class OperatorSyntaxValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Diffs #7, #10: after DROP SCHEMA, OPERATOR(pg_catalog.+)(1,2) should fail
    // PG: these run after DROP SCHEMA compat CASCADE where search_path = compat, pg_catalog
    // and compat no longer exists. PG errors; memgres succeeds.
    @Test void operator_qualified_plus_after_schema_drop() throws SQLException {
        exec("CREATE SCHEMA op_test");
        exec("SET search_path = op_test, pg_catalog");
        exec("DROP SCHEMA op_test CASCADE");
        // search_path has non-existent schema; PG errors on OPERATOR() in this state
        try {
            exec("SELECT OPERATOR(pg_catalog.+)(1,2)");
            fail("OPERATOR(pg_catalog.+)(1,2) should fail after schema drop in PG-compatible mode");
        } catch (SQLException e) {
            // Expected: PG returns an error here
        } finally {
            exec("SET search_path = public");
        }
    }

    // Diff #23: OPERATOR(pg_catalog.||)('a','b') should also fail in same context
    @Test void operator_qualified_concat_after_schema_drop() throws SQLException {
        exec("CREATE SCHEMA op_test2");
        exec("SET search_path = op_test2, pg_catalog");
        exec("DROP SCHEMA op_test2 CASCADE");
        try {
            exec("SELECT OPERATOR(pg_catalog.||)('a','b')");
            fail("OPERATOR(pg_catalog.||)('a','b') should fail after schema drop");
        } catch (SQLException e) {
            // Expected
        } finally {
            exec("SET search_path = public");
        }
    }

    // Additional: In PG18, OPERATOR(pg_catalog.+)(1,2) is parsed as unary + on ROW(1,2)
    @Test void operator_qualified_works_normally() {
        // PG18 treats this as unary operator on ROW(1,2), which fails with SQLSTATE 42883
        assertThrows(SQLException.class,
            () -> exec("SELECT OPERATOR(pg_catalog.+)(1,2)"),
            "OPERATOR(pg_catalog.+)(1,2) should fail in PG18 (treated as unary operator)");
    }
}
