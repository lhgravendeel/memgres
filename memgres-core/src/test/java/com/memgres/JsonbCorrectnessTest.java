package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for JSONB correctness: equality, insertion, build functions.
 * These tests document PG18-compatible behavior.
 */
class JsonbCorrectnessTest {

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

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    @Test
    void jsonb_equality_ignores_key_order() throws SQLException {
        // JSONB objects are unordered, so different key order should be equal
        String result = q("SELECT '{\"b\":2,\"a\":1}'::jsonb = '{\"a\":1,\"b\":2}'::jsonb");
        assertEquals("t", result);
    }

    @Test
    void jsonb_build_object_serializes_arrays() throws SQLException {
        // Arrays should become JSON arrays, not quoted strings
        String result = q("SELECT jsonb_build_object('x', 1, 'y', ARRAY[1,2,3])");
        assertNotNull(result);
        // Should contain [1, 2, 3] as array, not "[1, 2, 3]" as string
        assertFalse(result.contains("\"["), "Array should not be quoted as string: " + result);
        assertTrue(result.contains("[1") || result.contains("[ 1"), "Should contain JSON array: " + result);
    }

    @Test
    void jsonb_insert_at_array_position() throws SQLException {
        // jsonb_insert should INSERT at position, not REPLACE
        String result = q("SELECT jsonb_insert('{\"a\":[1,2,3]}'::jsonb, '{a,1}', '77'::jsonb)");
        assertNotNull(result);
        // Should insert 77 at position 1: [1, 77, 2, 3]
        assertTrue(result.contains("77") && result.contains("2") && result.contains("3"),
                "Should insert, not replace: " + result);
    }

    @Test
    void jsonb_duplicate_keys_last_wins() throws SQLException {
        // JSONB deduplicates keys; last value wins
        String result = q("SELECT '{\"a\":1,\"a\":2}'::jsonb");
        assertNotNull(result);
        // Should be {"a": 2} (last value wins), not {"a": 1, "a": 2}
        assertTrue(result.contains("2"), "Last value should win: " + result);
        assertFalse(result.matches(".*\"a\".*\"a\".*"), "Should not have duplicate keys: " + result);
    }
}
