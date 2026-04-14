package com.memgres.compat;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for JSON_SERIALIZE bytea wire encoding via JDBC getString().
 *
 * The comparison framework uses rs.getString(i) uniformly for all column types.
 * For bytea columns (like JSON_SERIALIZE without RETURNING), the PG JDBC driver's
 * getString() decodes the hex representation to raw bytes, then converts to String.
 *
 * These tests verify Memgres matches PG's wire behavior for bytea getString().
 */
class JsonSerializeWireEncodingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========================================================================
    // getString() on JSON_SERIALIZE bytea — should return decoded JSON text
    // ========================================================================

    @Test
    void getStringReturnsDecodedJsonObject() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            // PG JDBC getString() on bytea decodes the hex → raw bytes → String
            // For JSON_SERIALIZE, the bytes are UTF-8 JSON text, so getString should return the JSON text
            assertEquals("{\"a\": 1}", val,
                    "getString() on JSON_SERIALIZE bytea should return decoded JSON text");
        }
    }

    @Test
    void getStringReturnsDecodedJsonArray() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('[1,2,3]'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("[1, 2, 3]", val,
                    "getString() on JSON_SERIALIZE array bytea should return decoded JSON text");
        }
    }

    @Test
    void getStringReturnsDecodedNestedJson() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":{\"b\":2}}'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("{\"a\": {\"b\": 2}}", val,
                    "getString() on nested JSON should return decoded text");
        }
    }

    @Test
    void getStringReturnsDecodedJsonScalar() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('\"hello\"'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("\"hello\"", val,
                    "getString() on JSON string scalar should return decoded text");
        }
    }

    @Test
    void getStringReturnsDecodedJsonNumber() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('42'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("42", val,
                    "getString() on JSON number scalar should return decoded text");
        }
    }

    @Test
    void getStringReturnsDecodedJsonBoolean() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('true'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("true", val,
                    "getString() on JSON boolean should return decoded text");
        }
    }

    @Test
    void getStringReturnsNullForNullInput() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE(NULL::jsonb) AS result")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1),
                    "getString() on NULL JSON_SERIALIZE should return null");
        }
    }

    @Test
    void getStringReturnsDecodedJsonNull() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('null'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("null", val,
                    "getString() on JSON null literal should return decoded text");
        }
    }

    @Test
    void getStringReturnsDecodedEmptyObject() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{}'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("{}", val,
                    "getString() on empty JSON object should return decoded text");
        }
    }

    @Test
    void getStringReturnsDecodedEmptyArray() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('[]'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("[]", val,
                    "getString() on empty JSON array should return decoded text");
        }
    }

    @Test
    void getStringReturnsDecodedJsonWithSpecialChars() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_SERIALIZE('{\"key with spaces\":\"value\\nwith\\nnewlines\"}'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val, "getString() on JSON with special chars should not return null");
            assertTrue(val.contains("key with spaces"), "Should contain the key");
        }
    }

    // ========================================================================
    // getBytes() should still work for raw byte access
    // ========================================================================

    @Test
    void getBytesReturnsUtf8JsonBytes() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next());
            byte[] bytes = rs.getBytes(1);
            assertNotNull(bytes);
            String decoded = new String(bytes, StandardCharsets.UTF_8);
            assertEquals("{\"a\": 1}", decoded,
                    "getBytes() decoded as UTF-8 should match JSON text");
        }
    }

    // ========================================================================
    // Column metadata
    // ========================================================================

    @Test
    void columnTypeIsText() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("text", md.getColumnTypeName(1),
                    "JSON_SERIALIZE without RETURNING should report text type");
        }
    }

    // ========================================================================
    // RETURNING text variant — getString should work directly
    // ========================================================================

    @Test
    void returningTextGetStringWorks() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb RETURNING text) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("{\"a\": 1}", val,
                    "JSON_SERIALIZE RETURNING text getString() should return JSON text");
        }
    }

    // ========================================================================
    // Complex JSON structures
    // ========================================================================

    @Test
    void getStringReturnsDecodedLargerJson() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_SERIALIZE('{\"name\":\"test\",\"values\":[1,2,3],\"nested\":{\"x\":true}}'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // jsonb normalizes key order alphabetically
            assertTrue(val.contains("\"name\""), "Should contain name key");
            assertTrue(val.contains("\"values\""), "Should contain values key");
            assertTrue(val.contains("\"nested\""), "Should contain nested key");
        }
    }

    // ========================================================================
    // Consistency: getString and getBytes should return equivalent data
    // ========================================================================

    @Test
    void getStringAndGetBytesAreConsistent() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next());
            String fromGetString = rs.getString(1);
            // Need a fresh result set since cursor already advanced
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next());
            byte[] fromGetBytes = rs.getBytes(1);
            String decoded = new String(fromGetBytes, StandardCharsets.UTF_8);
            // Both should give the same JSON text
            try (Statement s2 = conn.createStatement();
                 ResultSet rs2 = s2.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
                assertTrue(rs2.next());
                String fromGetString = rs2.getString(1);
                assertEquals(decoded, fromGetString,
                        "getString() and getBytes()+decode should return the same JSON text");
            }
        }
    }
}
