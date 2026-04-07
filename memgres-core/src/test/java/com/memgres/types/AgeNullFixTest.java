package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that age(NULL) returns NULL instead of crashing with NPE.
 * This is critical for the Describe Statement inference path where
 * $N::varchar::xid is replaced with NULL::varchar::xid → age(NULL).
 *
 * Without this fix, parameterized queries from database clients that use
 * age($2::varchar::xid) fail during Describe Statement.
 */
class AgeNullFixTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void age_null_returns_null() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT pg_catalog.age(NULL)")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test void age_null_xid_returns_null() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT pg_catalog.age(NULL::varchar::xid)")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test void age_xid_coalesce_with_null() throws SQLException {
        // Catalog introspection pattern: coalesce(nullif(greatest(age(NULL::varchar::xid), -1), -1), 2147483647)
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT coalesce(nullif(greatest(pg_catalog.age(NULL::varchar::xid), -1), -1), 2147483647)")) {
            assertTrue(rs.next());
            assertEquals(2147483647, rs.getInt(1));
        }
    }

    @Test void describe_inference_with_age() throws SQLException {
        // Simulate what describeStatement does: replace params with NULL, add LIMIT 0
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE age_test(id serial PRIMARY KEY, name text)");
        }
        try {
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("""
                    SELECT T.relkind, T.relname, T.oid, T.xmin
                    FROM pg_catalog.pg_class T
                    WHERE relnamespace = NULL::oid AND relkind IN ('r','m','v','f','p')
                    AND pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(NULL::varchar::xid), -1), -1), 2147483647)
                    LIMIT 0
                    """)) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(4, md.getColumnCount(), "Should infer 4 columns even with NULL params");
            }
        } finally {
            try (Statement s = conn.createStatement()) { s.execute("DROP TABLE age_test"); }
        }
    }
}
