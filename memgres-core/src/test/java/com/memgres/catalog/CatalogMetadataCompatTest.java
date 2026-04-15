package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compatibility tests covering PG vs Memgres differences from
 * catalog-metadata-functions.sql (4 diffs) and check-function-bodies.sql (2 diffs).
 *
 * These tests are INTENDED TO FAIL against Memgres, documenting known incompatibilities.
 */
class CatalogMetadataCompatTest {

    private static Memgres memgres;

    @BeforeAll
    static void setUp() throws SQLException {
        memgres = Memgres.builder().port(0).build().start();
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA catmeta_test");
            stmt.execute("CREATE FUNCTION catmeta_test.catmeta_add(a int, b int) RETURNS int AS $$ SELECT a + b; $$ LANGUAGE SQL");
            stmt.execute("CREATE TABLE catmeta_test.catmeta_tbl (id int PRIMARY KEY, name text)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP SCHEMA catmeta_test CASCADE");
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(),
                memgres.getPassword()
        );
    }

    // ---------------------------------------------------------------
    // catalog-metadata-functions.sql differences
    // ---------------------------------------------------------------

    @Test
    void testPgDescribeObjectForFunctionIsNotEmpty() throws SQLException {
        // PG returns a non-empty description for a function via pg_describe_object.
        // Memgres returns empty/null. This test asserts PG behavior (expected to fail on Memgres).
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT pg_describe_object(1255, oid, 0) IS NOT NULL " +
                     "AND pg_describe_object(1255, oid, 0) <> '' AS has_desc " +
                     "FROM pg_proc WHERE proname = 'catmeta_add' " +
                     "AND pronamespace = 'catmeta_test'::regnamespace")) {
            assertTrue(rs.next(), "Expected a result row");
            assertTrue(rs.getBoolean("has_desc"),
                    "pg_describe_object should return a non-empty description for a function");
        }
    }

    @Test
    void testPgDescribeObjectLikeForFunction() throws SQLException {
        // PG returns a description containing the function name.
        // Memgres does not. This test asserts PG behavior (expected to fail on Memgres).
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT pg_describe_object(1255, oid, 0) LIKE '%catmeta_add%' AS desc_has_func " +
                     "FROM pg_proc WHERE proname = 'catmeta_add' " +
                     "AND pronamespace = 'catmeta_test'::regnamespace")) {
            assertTrue(rs.next(), "Expected a result row");
            assertTrue(rs.getBoolean("desc_has_func"),
                    "pg_describe_object description should contain the function name");
        }
    }

    @Test
    void testPgDescribeObjectForTableIsNotEmpty() throws SQLException {
        // PG returns a non-empty description for a table via pg_describe_object.
        // Memgres returns empty/null. This test asserts PG behavior (expected to fail on Memgres).
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT pg_describe_object(1259, oid, 0) IS NOT NULL " +
                     "AND pg_describe_object(1259, oid, 0) <> '' AS has_desc " +
                     "FROM pg_class WHERE relname = 'catmeta_tbl' " +
                     "AND relnamespace = 'catmeta_test'::regnamespace")) {
            assertTrue(rs.next(), "Expected a result row");
            assertTrue(rs.getBoolean("has_desc"),
                    "pg_describe_object should return a non-empty description for a table");
        }
    }

    @Test
    void testPgDescribeObjectForSchemaIsNotEmpty() throws SQLException {
        // PG returns a non-empty description for a schema via pg_describe_object.
        // Memgres returns empty/null. This test asserts PG behavior (expected to fail on Memgres).
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT pg_describe_object(2615, oid, 0) IS NOT NULL " +
                     "AND pg_describe_object(2615, oid, 0) <> '' AS has_desc " +
                     "FROM pg_namespace WHERE nspname = 'catmeta_test'")) {
            assertTrue(rs.next(), "Expected a result row");
            assertTrue(rs.getBoolean("has_desc"),
                    "pg_describe_object should return a non-empty description for a schema");
        }
    }

    // ---------------------------------------------------------------
    // check-function-bodies.sql differences
    // ---------------------------------------------------------------

    @Test
    void testShowCheckFunctionBodiesOffIsLowercase() throws SQLException {
        // PG returns 'off' (lowercase) for SHOW check_function_bodies after SET ... = off.
        // Memgres returns 'OFF' (uppercase). This test asserts PG behavior (expected to fail on Memgres).
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET check_function_bodies = off");
            try (ResultSet rs = stmt.executeQuery("SHOW check_function_bodies")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals("off", rs.getString(1),
                        "SHOW check_function_bodies should return lowercase 'off'");
            }
        }
    }

    @Test
    void testShowCheckFunctionBodiesOnIsLowercase() throws SQLException {
        // PG returns 'on' (lowercase) for SHOW check_function_bodies after SET ... = on.
        // Memgres returns 'ON' (uppercase). This test asserts PG behavior (expected to fail on Memgres).
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET check_function_bodies = on");
            try (ResultSet rs = stmt.executeQuery("SHOW check_function_bodies")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals("on", rs.getString(1),
                        "SHOW check_function_bodies should return lowercase 'on'");
            }
        }
    }
}
