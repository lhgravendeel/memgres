package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for 1 Memgres vs annotation mismatch from aggregate-functions.sql.
 *
 * Stmt 79: CREATE AGGREGATE with nonexistent SFUNC — error message should
 * include parameter types.
 *
 * Uses default JDBC (extended query protocol) to match the comparison framework.
 */
class AggregateErrorMessageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 79: CREATE AGGREGATE with nonexistent SFUNC should include
     * parameter types in the error message.
     *
     * PG: ERROR [42883] function agg_nonexistent_fn(integer, integer) does not exist
     * Memgres: ERROR [42883] function agg_nonexistent_fn does not exist
     */
    @Test
    void stmt79_aggregateNonexistentSfuncErrorShouldIncludeParamTypes() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE AGGREGATE agg_bad(integer) ("
                    + "SFUNC = agg_nonexistent_fn, STYPE = integer)");
            fail("Expected error 42883 for nonexistent SFUNC");
        } catch (SQLException e) {
            assertEquals("42883", e.getSQLState(),
                    "SQLSTATE should be 42883 (undefined_function)");
            assertTrue(e.getMessage().contains("agg_nonexistent_fn(integer, integer)"),
                    "Error message should include parameter types "
                    + "'agg_nonexistent_fn(integer, integer)', got: " + e.getMessage());
        }
    }
}
