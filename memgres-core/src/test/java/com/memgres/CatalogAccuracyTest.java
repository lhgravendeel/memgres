package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for system catalog accuracy, ensuring metadata queries return
 * PG18-compatible results.
 */
class CatalogAccuracyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // 1. ::regtype OID-to-name conversion
    // ========================================================================

    @Test
    void regtype_cast_converts_oid_to_type_name() throws SQLException {
        exec("CREATE TABLE regtype_test (id INT, name TEXT, ts TIMESTAMPTZ, flag BOOLEAN)");
        try {
            // pg_attribute.atttypid is an OID (integer). ::regtype should convert to type name.
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT a.attname, a.atttypid::regtype FROM pg_attribute a " +
                     "WHERE a.attrelid = 'regtype_test'::regclass AND a.attnum > 0 ORDER BY a.attnum")) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString(1));
                assertEquals("integer", rs.getString(2));

                assertTrue(rs.next());
                assertEquals("name", rs.getString(1));
                assertEquals("text", rs.getString(2));

                assertTrue(rs.next());
                assertEquals("ts", rs.getString(1));
                assertEquals("timestamp with time zone", rs.getString(2));

                assertTrue(rs.next());
                assertEquals("flag", rs.getString(1));
                assertEquals("boolean", rs.getString(2));
            }
        } finally {
            exec("DROP TABLE regtype_test");
        }
    }

    @Test
    void regtype_cast_known_oids() throws SQLException {
        // Direct OID-to-name conversion
        assertEquals("integer", querySingle("SELECT 23::regtype"));
        assertEquals("text", querySingle("SELECT 25::regtype"));
        assertEquals("boolean", querySingle("SELECT 16::regtype"));
        assertEquals("bigint", querySingle("SELECT 20::regtype"));
    }

    // ========================================================================
    // 2. column_default formatting
    // ========================================================================

    @Test
    void column_default_string_literal_has_type_cast() throws SQLException {
        exec("CREATE TABLE coldef_test (id INT, name TEXT DEFAULT 'hello')");
        try {
            String def = querySingle(
                "SELECT column_default FROM information_schema.columns " +
                "WHERE table_name = 'coldef_test' AND column_name = 'name'");
            assertNotNull(def);
            // PG formats string defaults with type cast: 'hello'::text
            assertTrue(def.contains("::"), "Default should have type cast: " + def);
            assertTrue(def.contains("hello"), "Default should contain the value: " + def);
        } finally {
            exec("DROP TABLE coldef_test");
        }
    }

    @Test
    void column_default_current_timestamp_uppercase() throws SQLException {
        exec("CREATE TABLE coldef_ts (id INT, created_at TIMESTAMPTZ DEFAULT current_timestamp)");
        try {
            String def = querySingle(
                "SELECT column_default FROM information_schema.columns " +
                "WHERE table_name = 'coldef_ts' AND column_name = 'created_at'");
            assertNotNull(def);
            // PG shows CURRENT_TIMESTAMP (uppercase)
            assertEquals("CURRENT_TIMESTAMP", def);
        } finally {
            exec("DROP TABLE coldef_ts");
        }
    }

    @Test
    void column_default_identity_shows_null() throws SQLException {
        exec("CREATE TABLE coldef_ident (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        try {
            String def = querySingle(
                "SELECT column_default FROM information_schema.columns " +
                "WHERE table_name = 'coldef_ident' AND column_name = 'id'");
            // PG shows NULL for identity columns (the identity is shown in other columns)
            assertNull(def, "Identity column default should be NULL, got: " + def);
        } finally {
            exec("DROP TABLE coldef_ident");
        }
    }

    @Test
    void column_default_enum_has_type_cast() throws SQLException {
        exec("CREATE TYPE test_status AS ENUM ('new', 'done')");
        exec("CREATE TABLE coldef_enum (id INT, status test_status DEFAULT 'new')");
        try {
            String def = querySingle(
                "SELECT column_default FROM information_schema.columns " +
                "WHERE table_name = 'coldef_enum' AND column_name = 'status'");
            assertNotNull(def);
            // PG formats: 'new'::test_status
            assertTrue(def.contains("::"), "Enum default should have type cast: " + def);
        } finally {
            exec("DROP TABLE coldef_enum");
            exec("DROP TYPE test_status");
        }
    }
}
